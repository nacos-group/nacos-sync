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
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.DubboConstants;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacossync.util.NacosUtils.getGroupNameOrDefault;
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

    private final MetricsManager metricsManager;

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

  
    public NacosSyncToZookeeperServiceImpl(SkyWalkerCacheServices skyWalkerCacheServices,
        NacosServerHolder nacosServerHolder, ZookeeperServerHolder zookeeperServerHolder, MetricsManager metricsManager) {
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.zookeeperServerHolder = zookeeperServerHolder;
        this.metricsManager = metricsManager;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        try {

            NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId());
            EventListener eventListener = nacosListenerMap.remove(taskDO.getTaskId());
            PathChildrenCache pathChildrenCache = pathChildrenCacheMap.get(taskDO.getTaskId());
            sourceNamingService.unsubscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                eventListener);
            CloseableUtils.closeQuietly(pathChildrenCache);
            Set<String> instanceUrlSet = instanceBackupMap.get(taskDO.getTaskId());
            CuratorFramework client = zookeeperServerHolder.get(taskDO.getDestClusterId());
            if(!instanceUrlSet.isEmpty()){
                for (String instanceUrl : instanceUrlSet) {
                    client.delete().quietly().forPath(instanceUrl);
                }
            }
        } catch (Exception e) {
            log.error("delete task from nacos to zk was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO, Integer index) {
        try {
            NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId());
            CuratorFramework client = zookeeperServerHolder.get(taskDO.getDestClusterId());
            nacosListenerMap.putIfAbsent(taskDO.getTaskId(), event -> {
                if (event instanceof NamingEvent) {
                    try {

                        List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(),
                            getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
                        Set<String> newInstanceUrlSet = getWaitingToAddInstance(taskDO, client, sourceInstances);

                        // fetch the instance backup
                        deleteInvalidInstances(taskDO, client, newInstanceUrlSet);
                        // replace the instance backup
                        instanceBackupMap.put(taskDO.getTaskId(), newInstanceUrlSet);
                        // try to compensate for the removed instance
                        tryToCompensate(taskDO, sourceNamingService, sourceInstances);
                    } catch (Exception e) {
                        log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);

                    }
                }
            });

            sourceNamingService.subscribe(taskDO.getServiceName(),getGroupNameOrDefault(taskDO.getGroupName()),
                nacosListenerMap.get(taskDO.getTaskId()));
        } catch (Exception e) {
            log.error("sync task from nacos to zk was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }
    
    private void tryToCompensate(TaskDO taskDO, NamingService sourceNamingService, List<Instance> sourceInstances) {
        if (!CollectionUtils.isEmpty(sourceInstances)) {
            final PathChildrenCache pathCache = getPathCache(taskDO);
            // Avoiding re-registration if there is already a listener registered.
            if (pathCache.getListenable().size() == 0) {
                registerCompensationListener(pathCache, taskDO, sourceNamingService);
            }
        }
    }
    
    private void registerCompensationListener(PathChildrenCache pathCache, TaskDO taskDO, NamingService sourceNamingService) {
        pathCache.getListenable().addListener((zkClient, zkEvent) -> {
            try {
                if (zkEvent.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    compensateOnChildRemoval(zkClient, zkEvent, sourceNamingService, taskDO);
                }
            } catch (Exception e) {
                log.error("Error processing ZooKeeper event: {}", zkEvent.getType(), e);
            }
        });
    }
    
    private void compensateOnChildRemoval(CuratorFramework zkClient, PathChildrenCacheEvent zkEvent, NamingService sourceNamingService, TaskDO taskDO) {
        String zkInstancePath = null;
        try {
            List<Instance> allInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()));
            zkInstancePath = zkEvent.getData().getPath();
            for (Instance instance : allInstances) {
                String instanceUrl = buildSyncInstance(instance, taskDO);
                if (zkInstancePath.equals(instanceUrl)) {
                    zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                            .forPath(zkInstancePath);
                    log.info("Compensated by re-creating the removed node at path: {}", zkInstancePath);
                    break;
                }
            }
        } catch (Exception e) {
            log.error("Failed to compensate for the removed node at path: {}", zkInstancePath, e);
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
        Map<String, String> metaData = new HashMap<>(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTER_ID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, taskDO.getSourceClusterId());

        String servicePath = monitorPath.computeIfAbsent(taskDO.getTaskId(),
            key -> convertDubboProvidersPath(metaData.get(DubboConstants.INTERFACE_KEY)));

        return convertDubboFullPathForZk(metaData, servicePath, instance.getIp(), instance.getPort());
    }


    /**
     * fetch zk path cache
     *
     * @param taskDO task instance
     * @return zk path cache
     */
    private PathChildrenCache getPathCache(TaskDO taskDO) {
        return pathChildrenCacheMap.computeIfAbsent(taskDO.getTaskId(), (key) -> {
            try {
                PathChildrenCache pathChildrenCache = new PathChildrenCache(
                    zookeeperServerHolder.get(taskDO.getDestClusterId()), monitorPath.get(key), false);
                pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                return pathChildrenCache;
            } catch (Exception e) {
                log.error("zookeeper path children cache start failed, taskId:{}", taskDO.getTaskId(), e);
                return null;
            }
        });

    }


}
