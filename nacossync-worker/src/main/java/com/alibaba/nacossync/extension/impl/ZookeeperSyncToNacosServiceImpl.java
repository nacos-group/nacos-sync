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

import static com.alibaba.nacossync.util.DubboConstants.ALL_SERVICE_NAME_PATTERN;
import static com.alibaba.nacossync.util.DubboConstants.DUBBO_PATH_FORMAT;
import static com.alibaba.nacossync.util.DubboConstants.DUBBO_ROOT_PATH;
import static com.alibaba.nacossync.util.DubboConstants.GROUP_KEY;
import static com.alibaba.nacossync.util.DubboConstants.INSTANCE_IP_KEY;
import static com.alibaba.nacossync.util.DubboConstants.INSTANCE_PORT_KEY;
import static com.alibaba.nacossync.util.DubboConstants.INTERFACE_KEY;
import static com.alibaba.nacossync.util.DubboConstants.PROTOCOL_KEY;
import static com.alibaba.nacossync.util.DubboConstants.VERSION_KEY;
import static com.alibaba.nacossync.util.DubboConstants.WEIGHT_KEY;
import static com.alibaba.nacossync.util.DubboConstants.ZOOKEEPER_SEPARATOR;
import static com.alibaba.nacossync.util.DubboConstants.createServiceName;
import static com.alibaba.nacossync.util.StringUtils.parseIpAndPortString;
import static com.alibaba.nacossync.util.StringUtils.parseQueryString;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import org.springframework.beans.factory.annotation.Autowired;

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

    @Autowired
    public ZookeeperSyncToNacosServiceImpl(ZookeeperServerHolder zookeeperServerHolder,
                                           NacosServerHolder nacosServerHolder, SkyWalkerCacheServices skyWalkerCacheServices) {
        this.zookeeperServerHolder = zookeeperServerHolder;
        this.nacosServerHolder = nacosServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            if (treeCacheMap.containsKey(taskDO.getTaskId())) {
                return true;
            }

            TreeCache treeCache = getTreeCache(taskDO);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            // 初次执行任务统一注册所有实例
            registerAllInstances(taskDO, destNamingService);
            //注册ZK监听
            Objects.requireNonNull(treeCache).getListenable().addListener((client, event) -> {
                try {

                    String path = event.getData().getPath();
                    Map<String, String> queryParam = parseQueryString(path);
                    if (isMatch(taskDO, queryParam) && needSync(queryParam)) {
                        processEvent(taskDO, destNamingService, event, path, queryParam);
                    }
                } catch (Exception e) {
                    log.error("event process from Zookeeper to Nacos was failed, taskId:{}", taskDO.getTaskId(), e);
                    metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                }

            });
        } catch (Exception e) {
            log.error("sync task from Zookeeper to Nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private void processEvent(TaskDO taskDO, NamingService destNamingService, TreeCacheEvent event, String path,
                              Map<String, String> queryParam) throws NacosException {
        if(!com.alibaba.nacossync.util.StringUtils.isDubboProviderPath(path)) {
            return;
        }

        Map<String, String> ipAndPortParam = parseIpAndPortString(path);
        Instance instance = buildSyncInstance(queryParam, ipAndPortParam, taskDO);
        String serviceName = queryParam.get(INTERFACE_KEY);
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
    }

    private void registerAllInstances(TaskDO taskDO, NamingService destNamingService) throws Exception {
        CuratorFramework zk =  zookeeperServerHolder.get(taskDO.getSourceClusterId(), "");
        if(!ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName())) {
            registerALLInstances0(taskDO, destNamingService, zk, taskDO.getServiceName());
        } else {
            // 同步全部
            List<String> serviceList = zk.getChildren().forPath(DUBBO_ROOT_PATH);
            for(String serviceName : serviceList) {
                registerALLInstances0(taskDO, destNamingService, zk, serviceName);
            }
        }
    }

    private void registerALLInstances0(TaskDO taskDO, NamingService destNamingService, CuratorFramework zk,
                                   String serviceName) throws Exception {
        String path = String.format(DUBBO_PATH_FORMAT, serviceName);
        if(zk.getChildren()==null) {
            return;
        }
        List<String> providers = zk.getChildren().forPath(path);
        for(String provider : providers) {
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

            CloseableUtils.closeQuietly(treeCacheMap.get(taskDO.getTaskId()));
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            if(!ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName())) {
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
                for(String serviceName : serviceNames) {

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
        Predicate<TaskDO> isVersionEq = (task) -> StringUtils.isBlank(taskDO.getVersion())
            || StringUtils.equals(task.getVersion(), queryParam.get(VERSION_KEY));
        Predicate<TaskDO> isGroupEq = (task) -> StringUtils.isBlank(taskDO.getGroupName())
            || StringUtils.equals(task.getGroupName(), queryParam.get(GROUP_KEY));
        return isVersionEq.and(isGroupEq).test(taskDO);
    }

    /**
     * create Nacos service instance
     *
     * @param queryParam dubbo metadata
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
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }

    /**
     * cteate Dubbo service name
     *
     * @param serviceName dubbo service name
     * @param queryParam dubbo metadata
     */
    protected String getServiceNameFromCache(String serviceName, Map<String, String> queryParam) {
        return nacosServiceNameMap.computeIfAbsent(serviceName, (key) -> createServiceName(queryParam));
    }

}
