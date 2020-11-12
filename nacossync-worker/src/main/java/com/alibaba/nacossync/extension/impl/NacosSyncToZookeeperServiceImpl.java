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
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.extension.impl.extend.NacosSyncToZookeeperServicesSharding;
import com.alibaba.nacossync.extension.impl.extend.Sharding;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.DubboConstants;
import com.alibaba.nacossync.util.ExpirySet;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.alibaba.nacossync.util.StringUtils.convertDubboFullPathForZk;
import static com.alibaba.nacossync.util.StringUtils.convertDubboProvidersPath;

/**
 * Nacos 同步 Zk 数据
 *
 * @author paderlol
 * @date 2019年01月06日, 15:08:06
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.ZK)
public class NacosSyncToZookeeperServiceImpl implements SyncService {

    @Autowired
    private MetricsManager metricsManager;

    /**
     * @description The Nacos listener map.
     */
    private final Map<String, EventListener> nacosListenerMap = new ConcurrentHashMap<>();
    /**
     * instance backup
     */
    private final Map<String, Set<String>> instanceBackupMap = new ConcurrentHashMap<>();

    /**
     * listener cache of zookeeper format: taskId -> PathChildrenCache instance
     */
    private final Map<String, PathChildrenCache> pathChildrenCacheMap = new ConcurrentHashMap<>();

    /**
     * zookeeper path for dubbo providers
     */
    private final Map<String, String> monitorPath = new ConcurrentHashMap<>();
    /**
     * @description The Sky walker cache services.
     */
    private final SkyWalkerCacheServices skyWalkerCacheServices;

    /**
     * @description The Nacos server holder.
     */
    private final NacosServerHolder nacosServerHolder;

    private final ZookeeperServerHolder zookeeperServerHolder;

    private static ExpirySet<String> serviceNameSet = new ExpirySet<String>();

    private static ExecutorService EXECUTOR = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @Resource(type = NacosSyncToZookeeperServicesSharding.class)
    private Sharding sharding;

    @Autowired
    public NacosSyncToZookeeperServiceImpl(SkyWalkerCacheServices skyWalkerCacheServices,
                                           NacosServerHolder nacosServerHolder, ZookeeperServerHolder zookeeperServerHolder) {
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.zookeeperServerHolder = zookeeperServerHolder;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            NamingService sourceNamingService =
                    //nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());//
                    nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getNameSpace());//fix with no nameSpaceName
            EventListener eventListener = nacosListenerMap.remove(taskDO.getTaskId());
            PathChildrenCache pathChildrenCache = pathChildrenCacheMap.get(taskDO.getTaskId());
            sourceNamingService.unsubscribe(taskDO.getServiceName(), eventListener);
            CloseableUtils.closeQuietly(pathChildrenCache);
            Set<String> instanceUrlSet = instanceBackupMap.get(taskDO.getTaskId());
            CuratorFramework client = zookeeperServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());
            for (String instanceUrl : instanceUrlSet) {
                client.delete().quietly().forPath(instanceUrl);
            }
            sharding.stop(taskDO);
        } catch (Exception e) {
            log.error("delete task from nacos to zk was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        sharding.start(taskDO);
        return true;
    }

    private void tryToCompensate(TaskDO taskDO, NamingService sourceNamingService, List<Instance> sourceInstances) {
        if (!CollectionUtils.isEmpty(sourceInstances)) {
            final PathChildrenCache pathCache = getPathCache(taskDO);
            if (pathCache.getListenable().size() == 0) { // 防止重复注册
                pathCache.getListenable().addListener((zkClient, zkEvent) -> {
                    if (zkEvent.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                        List<Instance> allInstances =
                                sourceNamingService.getAllInstances(taskDO.getServiceName());
                        for (Instance instance : allInstances) {
                            String instanceUrl = buildSyncInstance(instance, taskDO);
                            String zkInstancePath = zkEvent.getData().getPath();
                            if (zkInstancePath.equals(instanceUrl)) {
                                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                                        .forPath(zkInstancePath);
                                break;
                            }
                        }
                    }
                });
            }

        }
    }

    private void deleteInvalidInstances(TaskDO taskDO, CuratorFramework client, Set<String> newInstanceUrlSet)
            throws Exception {
        Set<String> instanceBackup =
                instanceBackupMap.getOrDefault(taskDO.getTaskId(), Sets.newHashSet());
        for (String instanceUrl : instanceBackup) {
            if (newInstanceUrlSet.contains(instanceUrl)) {
                continue;
            }
            client.delete().quietly().forPath(instanceUrl);
        }
    }

    private HashSet<String> getWaitingToAddInstance(TaskDO taskDO, CuratorFramework client,
                                                    List<Instance> sourceInstances) throws Exception {
        HashSet<String> waitingToAddInstance = new HashSet<>();
        for (Instance instance : sourceInstances) {
            if (needSync(instance.getMetadata())) {
                log.info("nacos->zk ,real sync service :{},and instance :{}", instance.getServiceName(), instance.getIp());
                String instanceUrl = buildSyncInstance(instance, taskDO);
                if (null == client.checkExists().forPath(instanceUrl)) {
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                            .forPath(instanceUrl);
                }
                waitingToAddInstance.add(instanceUrl);
            }
        }
        return waitingToAddInstance;
    }

    protected String buildSyncInstance(Instance instance, TaskDO taskDO) throws UnsupportedEncodingException {
        Map<String, String> metaData = new HashMap<>();
        metaData.putAll(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());

        String servicePath = monitorPath.computeIfAbsent(taskDO.getTaskId(),
                key -> convertDubboProvidersPath(metaData.get(DubboConstants.INTERFACE_KEY)));

        return convertDubboFullPathForZk(metaData, servicePath, instance.getIp(), instance.getPort());
    }


    /**
     * 获取zk path child 监听缓存类
     *
     * @param taskDO 任务对象
     * @return zk节点操作缓存对象
     */
    private PathChildrenCache getPathCache(TaskDO taskDO) {
        return pathChildrenCacheMap.computeIfAbsent(taskDO.getTaskId(), (key) -> {
            try {
                PathChildrenCache pathChildrenCache = new PathChildrenCache(
                        zookeeperServerHolder.get(taskDO.getDestClusterId(), ""), monitorPath.get(key), false);
                pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                return pathChildrenCache;
            } catch (Exception e) {
                log.error("zookeeper path children cache start failed, taskId:{}", taskDO.getTaskId(), e);
                return null;
            }
        });

    }


    private class SyncThread implements Runnable {

        NamingService sourceNamingService;

        TaskDO taskDO;

        CuratorFramework client;

        SyncThread(NamingService sourceNamingService, TaskDO taskDO, CuratorFramework client) {
            this.sourceNamingService = sourceNamingService;
            this.taskDO = taskDO;
            this.client = client;
        }

        @Override
        public void run() {
            try {

                //List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName());
                List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(), taskDO.getGroupName());//fix  with no group
                Set<String> newInstanceUrlSet = getWaitingToAddInstance(taskDO, client, sourceInstances);

                // 获取之前的备份 删除无效实例
                deleteInvalidInstances(taskDO, client, newInstanceUrlSet);
                // 替换当前备份为最新备份
                instanceBackupMap.put(taskDO.getTaskId(), newInstanceUrlSet);
                // 尝试恢复因为zk客户端意外断开导致的实例数据
                tryToCompensate(taskDO, sourceNamingService, filterNeedSync(sourceInstances));
            } catch (Exception e) {
                log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);

            } finally {
                //serviceNameSet.remove(((NamingEvent) event).getServiceName());//如果考虑高实时性 可以手动remove 这样时间窗口的大小就不固定 依赖处理速度 窗口大小作为兜底
            }
        }
    }

    private List<Instance> filterNeedSync(List<Instance> sourceInstances) {
        Iterator<Instance> iterator = sourceInstances.iterator();
        while (iterator.hasNext()) {
            if (!needSync(iterator.next().getMetadata())) {
                iterator.remove();
            }
        }
        return sourceInstances;
    }

    public boolean addSynService(TaskDO taskDO) {
        try {
            NamingService sourceNamingService =
                    //nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());
                    nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getNameSpace());//fix with no nameSpaceName
            CuratorFramework client = zookeeperServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());
            nacosListenerMap.putIfAbsent(taskDO.getTaskId(), event -> {
                if (event instanceof NamingEvent) {
                    if (serviceNameSet.set(((NamingEvent) event).getServiceName())) {// add event merge
                        EXECUTOR.execute(new SyncThread(sourceNamingService, taskDO, client));
                    }
                }
            });
            sourceNamingService.subscribe(taskDO.getServiceName(), nacosListenerMap.get(taskDO.getTaskId()));
        } catch (Exception e) {
            log.error("sync task from nacos to zk was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

}
