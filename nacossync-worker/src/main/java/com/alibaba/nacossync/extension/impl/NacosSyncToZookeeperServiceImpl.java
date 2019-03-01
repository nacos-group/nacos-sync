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
import org.springframework.beans.factory.annotation.Autowired;

import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
                nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());
            EventListener eventListener = nacosListenerMap.remove(taskDO.getTaskId());
            PathChildrenCache pathChildrenCache = pathChildrenCacheMap.get(taskDO.getTaskId());
            sourceNamingService.unsubscribe(taskDO.getServiceName(), eventListener);
            CloseableUtils.closeQuietly(pathChildrenCache);
            Set<String> instanceUrlSet = instanceBackupMap.get(taskDO.getTaskId());
            CuratorFramework client = zookeeperServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());
            for (String instanceUrl : instanceUrlSet) {
                client.delete().quietly().forPath(instanceUrl);
            }
        } catch (Exception e) {
            log.error("delete task from nacos to zk was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());
            CuratorFramework client = zookeeperServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());
            nacosListenerMap.putIfAbsent(taskDO.getTaskId(), event -> {
                if (event instanceof NamingEvent) {
                    try {
                        Set<String> newInstanceUrlSet = new HashSet<>();
                        List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName());

                        for (Instance instance : sourceInstances) {
                            if (needSync(instance.getMetadata())) {
                                String instanceUrl = buildSyncInstance(instance, taskDO);
                                if (null == client.checkExists().forPath(instanceUrl)) {
                                    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                                        .forPath(instanceUrl);
                                }
                                newInstanceUrlSet.add(instanceUrl);
                            }
                        }

                        // 获取之前的备份 删除所有
                        Set<String> instanceBackup =
                            instanceBackupMap.getOrDefault(taskDO.getTaskId(), Sets.newHashSet());
                        for (String instanceUrl : instanceBackup) {
                            if (newInstanceUrlSet.contains(instanceUrl)) {
                                continue;
                            }
                            client.delete().quietly().forPath(instanceUrl);
                        }
                        // 替换当前备份为最新备份
                        instanceBackupMap.put(taskDO.getTaskId(), newInstanceUrlSet);
                        if (!CollectionUtils.isEmpty(sourceInstances)) {

                            getPathCache(taskDO).getListenable().addListener((zkClient, zkEvent) -> {

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
                    } catch (Exception e) {
                        log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);

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

    protected String buildSyncInstance(Instance instance, TaskDO taskDO) throws UnsupportedEncodingException {
        Map<String, String> metaData = new HashMap<>();
        metaData.putAll(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        int weight = Integer.parseInt(new DecimalFormat("0").format(Math.ceil(instance.getWeight())));
        metaData.put(DubboConstants.WEIGHT_KEY, Integer.toString(weight));

        String servicePath = monitorPath.computeIfAbsent(taskDO.getTaskId(),
            key -> convertDubboProvidersPath(metaData.get(DubboConstants.INTERFACE_KEY)));

        return convertDubboFullPathForZk(metaData, servicePath, instance.getIp(), instance.getPort());
    }

    /**
     * 获取zk path child 监听缓存类
     *
     * @param taskDO
     * @return
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

}
