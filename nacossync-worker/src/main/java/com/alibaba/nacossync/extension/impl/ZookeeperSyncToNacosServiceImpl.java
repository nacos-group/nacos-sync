/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.client.naming.NacosNamingService;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.extension.impl.extend.Sharding;
import com.alibaba.nacossync.extension.impl.extend.ZookeeperSyncToNacosServiceSharding;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.nacossync.constant.SkyWalkerConstants.SOURCE_CLUSTERID_KEY;
import static com.alibaba.nacossync.util.DubboConstants.*;
import static com.alibaba.nacossync.util.StringUtils.parseIpAndPortString;
import static com.alibaba.nacossync.util.StringUtils.parseQueryString;
import static com.alibaba.nacossync.util.ZookeeperUtils.filterNoProviderPath;

/**
 * @author paderlol
 * @version 1.0
 * @date: 2018-12-24 21:33
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.ZK, destinationCluster = ClusterTypeEnum.NACOS)
public class ZookeeperSyncToNacosServiceImpl implements SyncService {

    @Autowired
    private MetricsManager metricsManager;

    /**
     * Listener cache of Zookeeper format taskId -> PathChildrenCache instance
     */
    private Map<String, TreeCache> treeCacheMap = new ConcurrentHashMap<>();
    /**
     * service name cache
     */
    private Map<String, String> nacosServiceNameMap = new ConcurrentHashMap<>();

    private final ZookeeperServerHolder zookeeperServerHolder;

    private final NacosServerHolder nacosServerHolder;

    private final SkyWalkerCacheServices skyWalkerCacheServices;

    @Resource(type = ZookeeperSyncToNacosServiceSharding.class)
    private Sharding sharding;

    //排除/dobbo下面的所有非服务节点
    private static final List<String> IGNORED_DUBBO_PATH = Stream.of("mapping", "metadata", "yellow").collect(Collectors.toList());

    private final BlockingQueue<EventWrapperPOJO> providersChangeEvent = new LinkedBlockingQueue<EventWrapperPOJO>();

    private ExecutorService executor = null;

    @Autowired
    public ZookeeperSyncToNacosServiceImpl(ZookeeperServerHolder zookeeperServerHolder,
                                           NacosServerHolder nacosServerHolder, SkyWalkerCacheServices skyWalkerCacheServices) {
        this.zookeeperServerHolder = zookeeperServerHolder;
        this.nacosServerHolder = nacosServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;

        executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "com.alibaba.nacossync.syncThread");
                thread.setDaemon(true);
                return thread;
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        EventWrapperPOJO eventWrapperPOJO = providersChangeEvent.poll(5, TimeUnit.MINUTES);
                        NamingService destNamingService = nacosServerHolder.get(eventWrapperPOJO.getTaskDO().getDestClusterId(), null);
                        sync(eventWrapperPOJO.getTaskDO(), destNamingService, eventWrapperPOJO.getEvent());
                    } catch (Exception e) {
                        log.error("deal with zk->nacos event error.", e);
                    }
                }
            }
        });
    }


    @Override
    public boolean sync(TaskDO taskDO) {
        sharding.start(taskDO);
        addSubscribe(taskDO);
        return true;
    }

    private void processEvent(TaskDO taskDO, NamingService destNamingService, TreeCacheEvent event, String path, Map<String, String> queryParam) {
        Map<String, String> ipAndPortParam = parseIpAndPortString(path);
        String serviceName = queryParam.get(INTERFACE_KEY);
        try {
            Instance instance = buildSyncInstance(queryParam, ipAndPortParam, taskDO);
            switch (event.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                    destNamingService.registerInstance(
                            getServiceNameFromCache(serviceName, queryParam), instance);
                    break;
                case NODE_REMOVED:
                    destNamingService.deregisterInstance(
                            getServiceNameFromCache(serviceName, queryParam),
                            ipAndPortParam.get(INSTANCE_IP_KEY),
                            Integer.parseInt(ipAndPortParam.get(INSTANCE_PORT_KEY)));
                    nacosServiceNameMap.remove(serviceName);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("While process zk event occur error.", e);
        }
    }

    private void registerALLInstances0(TaskDO taskDO, NamingService destNamingService, CuratorFramework zk,
                                       String serviceName) throws Exception {
        String path = String.format(DUBBO_PATH_FORMAT, serviceName);
        if (zk.getChildren() == null) {
            return;
        }
        List<String> providers = zk.getChildren().forPath(path);
        for (String provider : providers) {
            Map<String, String> queryParam = parseQueryString(provider);
            if (isMatch(taskDO, queryParam) && needSync(queryParam)) {
                Map<String, String> ipAndPortParam = parseIpAndPortString(path + ZOOKEEPER_SEPARATOR + provider);
                Instance instance = buildSyncInstance(queryParam, ipAndPortParam, taskDO);
                destNamingService.registerInstance(getServiceNameFromCache(serviceName, queryParam),
                        instance);
            }
        }
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        if (taskDO.getServiceName() == null) {
            return true;
        }
        try {
            // CloseableUtils.closeQuietly(treeCacheMap.get(taskDO.getTaskId()));
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            if (!ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName())) {
                if (nacosServiceNameMap.containsKey(taskDO.getServiceName())) {
                    List<Instance> allInstances =
                            destNamingService.getAllInstances(nacosServiceNameMap.get(taskDO.getServiceName()));
                    for (Instance instance : allInstances) {
                        if (needDelete(instance.getMetadata(), taskDO)) {
                            destNamingService.deregisterInstance(instance.getServiceName(), instance.getIp(),
                                    instance.getPort());
                        }
                        nacosServiceNameMap.remove(taskDO.getServiceName());

                    }
                }
            } else {
                Set<String> serviceNames = nacosServiceNameMap.keySet();
                for (String serviceName : serviceNames) {
                    if (nacosServiceNameMap.containsKey(serviceName)) {
                        List<Instance> allInstances =
                                destNamingService.getAllInstances(serviceName);
                        for (Instance instance : allInstances) {
                            if (needDelete(instance.getMetadata(), taskDO)) {
                                destNamingService.deregisterInstance(instance.getServiceName(), instance.getIp(),
                                        instance.getPort());
                            }
                            nacosServiceNameMap.remove(serviceName);

                        }
                    }
                }
            }


        } catch (Exception e) {
            log.error("delete task from zookeeper to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    /**
     * fetch the Path cache when the task sync
     */
    protected TreeCache getTreeCache(TaskDO taskDO) {
        return treeCacheMap.computeIfAbsent(taskDO.getTaskId(), (key) -> {
            try {
                TreeCache treeCache =
                        new TreeCache(zookeeperServerHolder.get(taskDO.getSourceClusterId(), ""),
                                DUBBO_ROOT_PATH);
                treeCache.start();
                return treeCache;
            } catch (Exception e) {
                log.error("zookeeper path children cache start failed, taskId:{}", taskDO.getTaskId(), e);
                return null;
            }
        });

    }

    /**
     * The instance information that needs to be synchronized is matched based on the dubbo version and the grouping
     * name
     */
    protected boolean isMatch(TaskDO taskDO, Map<String, String> queryParam) {
        //此处和社区不一致
       /*
        Predicate<TaskDO> isVersionEq = (task) -> StringUtils.isBlank(taskDO.getVersion())
                || StringUtils.equals(task.getVersion(), queryParam.get(VERSION_KEY));
        Predicate<TaskDO> isGroupEq = (task) -> StringUtils.isBlank(taskDO.getGroupName()) || StringUtils.isBlank(queryParam.get(GROUP_KEY)) //fix
                || StringUtils.equals(task.getGroupName(), queryParam.get(GROUP_KEY));
        return isVersionEq.and(isGroupEq).test(taskDO);*/
        return true;
    }

    /**
     * create Nacos service instance
     *
     * @param queryParam   dubbo metadata
     * @param ipAndPortMap dubbo ip and address
     */
    protected Instance buildSyncInstance(Map<String, String> queryParam, Map<String, String> ipAndPortMap,
                                         TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(ipAndPortMap.get(INSTANCE_IP_KEY));
        temp.setPort(Integer.parseInt(ipAndPortMap.get(INSTANCE_PORT_KEY)));
        temp.setServiceName(getServiceNameFromCache(taskDO.getTaskId(), queryParam));
        temp.setWeight(Double.parseDouble(queryParam.get(WEIGHT_KEY) == null ? "1.0" : queryParam.get(WEIGHT_KEY)));
        temp.setHealthy(true);

        Map<String, String> metaData = new HashMap<>(queryParam);
        metaData.put(PROTOCOL_KEY, ipAndPortMap.get(PROTOCOL_KEY));
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }

    /**
     * cteate Dubbo service name
     *
     * @param serviceName dubbo service name
     * @param queryParam  dubbo metadata
     */
    protected String getServiceNameFromCache(String serviceName, Map<String, String> queryParam) {
        //此处和社区不一致
        String name = Joiner.on(SEPARATOR_KEY).skipNulls().join(CATALOG_KEY, queryParam.get(INTERFACE_KEY),
                queryParam.get(VERSION_KEY) == null ? "" : queryParam.get(VERSION_KEY), queryParam.get(GROUP_KEY) == null ? "" : queryParam.get(GROUP_KEY));
        nacosServiceNameMap.putIfAbsent(serviceName, name);
        return name;
    }


    public void addSubscribe(TaskDO taskDO) {

        try {
            if (treeCacheMap.containsKey(taskDO.getTaskId())) {
                return;
            }
            TreeCache treeCache = getTreeCache(taskDO);
            Objects.requireNonNull(treeCache).getListenable().addListener((client, event) -> {
                try {
                    if (event.getData() == null) {
                        return;
                    }
                    String path = event.getData().getPath();
                    Map<String, String> queryParam = parseQueryString(path);
                    if (!needSync(queryParam)) {
                        return;
                    }
                    if (com.alibaba.nacossync.util.StringUtils.isDubboProviderPath(path)) {
                        sharding.reShardingIfNeed();
                        if (sharding.isProcess(taskDO, queryParam.get(INTERFACE_KEY))) {
                            providersChangeEvent.offer(new EventWrapperPOJO(event, taskDO));
                        }
                    }
                } catch (Exception e) {
                    log.error("sync task from Zookeeper to Nacos was failed, taskId:{}", taskDO.getTaskId(), e);
                    metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                }
            });
        } catch (Exception e) {
            log.error("zk->nacos subscribe faild ,taskid:{}", taskDO.getId(), e);
        }
    }

    private void sync(TaskDO taskDO, NamingService destNamingService, TreeCacheEvent event) throws Exception {
        String path = event.getData().getPath();
        Map<String, String> queryParam = parseQueryString(path);
        if (isMatch(taskDO, queryParam) && needSync(queryParam)) {
            processEvent(taskDO, destNamingService, event, path, queryParam);
        }

    }

    public void addSyncService(TaskDO taskDO) {
        try {
            if (!IGNORED_DUBBO_PATH.contains(taskDO.getServiceName())) {
                CuratorFramework zk = zookeeperServerHolder.get(taskDO.getSourceClusterId(), "");
                NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
                registerALLInstances0(taskDO, destNamingService, zk, taskDO.getServiceName());
            }
        } catch (Exception e) {
            log.error("zk->nacos addSyncService faild ,taskid:{}", taskDO.getId(), e);
        }
    }

    public void removeSyncServices(TaskDO taskDO) {
        try {
            NamingService namingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            List<Instance> allInstances =
                    namingService.getAllInstances(nacosServiceNameMap.get(taskDO.getServiceName()));
            for (Instance instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO)) {
                    ((NacosNamingService) namingService).getBeatReactor().removeBeatInfo(instance.getServiceName(), instance.getIp(), instance.getPort());//不能直接删除防止其他server注册的被删除
                }
                nacosServiceNameMap.remove(taskDO.getServiceName());
            }
        } catch (Exception e) {
            log.error("zk->nacos removeSyncServices faild ,taskid:{}", taskDO.getId(), e);
        }
    }

    public List<String> getAllServicesFromZk(TaskDO taskDO) {
        try {
            CuratorFramework zk = zookeeperServerHolder.get(taskDO.getSourceClusterId(), "");
            if (zk.checkExists().forPath(DUBBO_ROOT_PATH) == null) {
                zk.create().forPath(DUBBO_ROOT_PATH);
            }
            List<String> serviceList = zk.getChildren().forPath(DUBBO_ROOT_PATH);
            return filterNoProviderPath(serviceList);
        } catch (Exception e) {
            log.error("zk->nacos getAllServicesFromZk faild ,taskid:{}", taskDO.getId(), e);
        }
        return null;
    }

    private class EventWrapperPOJO {
        private TreeCacheEvent event;
        private TaskDO taskDO;

        EventWrapperPOJO(TreeCacheEvent event, TaskDO taskDO) {
            this.event = event;
            this.taskDO = taskDO;
        }

        public TreeCacheEvent getEvent() {
            return event;
        }

        public void setEvent(TreeCacheEvent event) {
            this.event = event;
        }

        public TaskDO getTaskDO() {
            return taskDO;
        }

        public void setTaskDO(TaskDO taskDO) {
            this.taskDO = taskDO;
        }
    }
}
