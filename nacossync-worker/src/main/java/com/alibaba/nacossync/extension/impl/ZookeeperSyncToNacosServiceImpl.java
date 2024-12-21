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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.utils.CloseableUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

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
import static com.alibaba.nacossync.util.NacosUtils.getGroupNameOrDefault;
import static com.alibaba.nacossync.util.StringUtils.parseIpAndPortString;
import static com.alibaba.nacossync.util.StringUtils.parseQueryString;

/**
 * @author paderlol
 * @version 1.0
 * @date: 2018-12-24 21:33
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.ZK, destinationCluster = ClusterTypeEnum.NACOS)
public class ZookeeperSyncToNacosServiceImpl implements SyncService {
    
    private static final String DEFAULT_WEIGHT = "1.0";
    
    private final MetricsManager metricsManager;
    
    /**
     * Listener cache of Zookeeper format taskId -> PathChildrenCache instance
     */
    private final Map<String, CuratorCache> treeCacheMap = new ConcurrentHashMap<>();
    
    /**
     * service name cache
     */
    private final Map<String, ConcurrentHashMap<String, String>> nacosServiceNameMap = new ConcurrentHashMap<>();
    
    private final ZookeeperServerHolder zookeeperServerHolder;
    
    private final NacosServerHolder nacosServerHolder;
    
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    
    
    public ZookeeperSyncToNacosServiceImpl(ZookeeperServerHolder zookeeperServerHolder,
            NacosServerHolder nacosServerHolder, SkyWalkerCacheServices skyWalkerCacheServices,
            MetricsManager metricsManager) {
        this.zookeeperServerHolder = zookeeperServerHolder;
        this.nacosServerHolder = nacosServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.metricsManager = metricsManager;
    }
    
    @Override
    public boolean sync(TaskDO taskDO, Integer index) {
        try {
            if (treeCacheMap.containsKey(taskDO.getTaskId())) {
                return true;
            }
            if (!initializeTreeCache(taskDO)) {
                return false;
            }
            
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());
            if (destNamingService == null) {
                logAndRecordSyncError("Failed to obtain NamingService for destination clusterId: {}",
                        taskDO.getDestClusterId(), null);
                return false;
            }
            // 初次执行任务统一注册所有实例
            registerAllInstances(taskDO, destNamingService);
            setupListener(taskDO, destNamingService);
        } catch (Exception e) {
            log.error("sync task from Zookeeper to Nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }
    
    private boolean initializeTreeCache(TaskDO taskDO) {
        CuratorCache treeCache = getTreeCache(taskDO);
        if (treeCache == null) {
            logAndRecordSyncError("Failed to obtain TreeCache for taskId: {}", taskDO.getTaskId(), null);
            return false;
        }
        return true;
    }
    
    private void logAndRecordSyncError(String message, String taskId, Exception e) {
        if (e != null) {
            log.error(message, taskId, e);
        } else {
            log.error(message, taskId);
        }
        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
    }
    
    private void setupListener(TaskDO taskDO, NamingService destNamingService) {
        CuratorCache treeCache = Objects.requireNonNull(getTreeCache(taskDO));
        treeCache.listenable().addListener((type, oldData, newData) -> {
            try {
                // INITIALIZED is a special event that is not triggered by the Zookeeper server
                if (newData == null) {
                    log.warn("TreeCache event data is null, taskId:{}", taskDO.getTaskId());
                    return;
                }
                String path = newData.getPath();
                Map<String, String> queryParam = parseQueryString(path);
                if (isMatch(taskDO, queryParam) && needSync(queryParam)) {
                    processEvent(taskDO, destNamingService, type, path, queryParam);
                }
            } catch (Exception e) {
                logAndRecordSyncError("Event process from Zookeeper to Nacos was failed, taskId:{}", taskDO.getTaskId(),
                        e);
            }
        });
    }
    
    
    public boolean delete(TaskDO taskDO) {
        if (taskDO.getServiceName() == null) {
            return true;
        }
        
        CloseableUtils.closeQuietly(treeCacheMap.remove(taskDO.getTaskId()));
        
        try {
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());
            if (destNamingService == null) {
                log.error("Failed to obtain NamingService for destination clusterId: {}", taskDO.getDestClusterId());
                return false;
            }
            
            Collection<String> serviceNames = getServiceNamesToDelete(taskDO);
            deleteInstances(serviceNames, destNamingService, taskDO);
            
        } catch (Exception e) {
            log.error("Delete task from Zookeeper to Nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        
        return true;
    }
    
    
    private Collection<String> getServiceNamesToDelete(TaskDO taskDO) {
        String taskId = taskDO.getTaskId();
        // Get the service map for the task
        ConcurrentHashMap<String, String> serviceMap = nacosServiceNameMap.get(taskId);
        // If the service map is null, there is no service to delete
        if (serviceMap == null) {
            return Collections.emptyList();
        }
        // If the service name is not specified, delete all services
        if (ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName())) {
            return nacosServiceNameMap.remove(taskId).values();
        }
        
        // Delete the specified service
        String removedService = serviceMap.remove(taskDO.getServiceName());
        return StringUtils.isNotBlank(removedService) ? Collections.singletonList(removedService) : Collections.emptyList();
    }

    
    private void deleteInstances(Collection<String> serviceNames, NamingService destNamingService, TaskDO taskDO) {
        for (String serviceName : serviceNames) {
            try {
                List<Instance> allInstances = destNamingService.getAllInstances(serviceName,
                        getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
                List<Instance> instances = allInstances.stream()
                        .filter(instance -> needDelete(instance.getMetadata(), taskDO)).toList();
                if (instances.isEmpty()) {
                    continue;
                }
                destNamingService.batchDeregisterInstance(serviceName, getGroupNameOrDefault(taskDO.getGroupName()),
                        instances);
                
            } catch (NacosException e) {
                log.error("Failed to deregister service instance for serviceName: {}", serviceName, e);
            }
        }
    }
    
    /**
     * fetch the Path cache when the task sync
     */
    protected CuratorCache getTreeCache(TaskDO taskDO) {
        return treeCacheMap.computeIfAbsent(taskDO.getTaskId(), (key) -> {
            try {
                
                CuratorCache curatorCache = CuratorCache.build(zookeeperServerHolder.get(taskDO.getSourceClusterId()),
                        DUBBO_ROOT_PATH);
                curatorCache.start();
                return curatorCache;
            } catch (Exception e) {
                log.error("zookeeper path children cache start failed, taskId:{}", taskDO.getTaskId(), e);
                return null;
            }
        });
        
    }
    
    /**
     * Checks if the instance information needs to be synchronized based on the dubbo version, grouping name, and
     * service name.
     */
    protected boolean isMatch(TaskDO taskDO, Map<String, String> queryParam) {
        return isVersionMatch(taskDO, queryParam) && isGroupMatch(taskDO, queryParam) && isServiceMatch(taskDO,
                queryParam) || isMatchAllServices(taskDO);
    }
    
    private boolean isVersionMatch(TaskDO task, Map<String, String> queryParam) {
        return StringUtils.isBlank(task.getVersion()) || StringUtils.equals(task.getVersion(),
                queryParam.get(VERSION_KEY));
    }
    
    private boolean isGroupMatch(TaskDO task, Map<String, String> queryParam) {
        return StringUtils.isBlank(task.getGroupName()) || StringUtils.equals(task.getGroupName(),
                queryParam.get(GROUP_KEY));
    }
    
    private boolean isServiceMatch(TaskDO task, Map<String, String> queryParam) {
        return StringUtils.isNotBlank(task.getServiceName()) && StringUtils.equals(task.getServiceName(),
                queryParam.get(INTERFACE_KEY));
    }
    
    private boolean isMatchAllServices(TaskDO task) {
        return StringUtils.isNotBlank(task.getServiceName()) && StringUtils.equals(task.getServiceName(),
                ALL_SERVICE_NAME_PATTERN);
    }
    
    
    /**
     * Builds a synchronized Nacos instance from Zookeeper data.
     *
     * @param queryParam   Parameters obtained from the query string.
     * @param ipAndPortMap IP and port information.
     * @param taskDO       Task details.
     * @return A fully configured Nacos instance.
     */
    protected Instance buildSyncInstance(Map<String, String> queryParam, Map<String, String> ipAndPortMap,
            TaskDO taskDO) {
        Instance instance = new Instance();
        instance.setIp(ipAndPortMap.get(INSTANCE_IP_KEY));
        instance.setPort(Integer.parseInt(ipAndPortMap.get(INSTANCE_PORT_KEY)));
        instance.setServiceName(getServiceNameFromCache(taskDO.getTaskId(), queryParam.get(INTERFACE_KEY), queryParam));
        instance.setWeight(parseWeight(queryParam));
        instance.setHealthy(true);
        instance.setMetadata(buildMetadata(queryParam, ipAndPortMap, taskDO));
        return instance;
    }
    
    
    private double parseWeight(Map<String, String> queryParam) {
        try {
            return Double.parseDouble(queryParam.getOrDefault(WEIGHT_KEY, DEFAULT_WEIGHT));
        } catch (NumberFormatException e) {
            log.error("Error parsing weight: {}", queryParam.get(WEIGHT_KEY), e);
            return Double.parseDouble(DEFAULT_WEIGHT);  // Default weight in case of error
        }
    }
    
    private Map<String, String> buildMetadata(Map<String, String> queryParam, Map<String, String> ipAndPortMap,
            TaskDO taskDO) {
        Map<String, String> metaData = new HashMap<>(queryParam);
        metaData.put(PROTOCOL_KEY, ipAndPortMap.get(PROTOCOL_KEY));
        metaData.put(SkyWalkerConstants.DEST_CLUSTER_ID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, taskDO.getSourceClusterId());
        return metaData;
    }
    
    private void processEvent(TaskDO taskDO, NamingService destNamingService, CuratorCacheListener.Type type,
            String path, Map<String, String> queryParam) throws NacosException {
        if (!com.alibaba.nacossync.util.StringUtils.isDubboProviderPath(path)) {
            return;
        }
        
        Map<String, String> ipAndPortParam = parseIpAndPortString(path);
        if (ipAndPortParam.isEmpty()) {
            log.error("Invalid IP and Port data extracted from path: {}", path);
            return;
        }
        Instance instance = buildSyncInstance(queryParam, ipAndPortParam, taskDO);
        String serviceName = queryParam.get(INTERFACE_KEY);
        if (serviceName == null || serviceName.isEmpty()) {
            log.error("Service name is missing in the query parameters.");
            return;
        }
        switch (type) {
            case NODE_CREATED:
            case NODE_CHANGED:
                
                destNamingService.batchRegisterInstance(
                        getServiceNameFromCache(taskDO.getTaskId(), serviceName, queryParam),
                        getGroupNameOrDefault(taskDO.getGroupName()), List.of(instance));
                break;
            case NODE_DELETED:
                
                destNamingService.deregisterInstance(
                        getServiceNameFromCache(taskDO.getTaskId(), serviceName, queryParam),
                        getGroupNameOrDefault(taskDO.getGroupName()), ipAndPortParam.get(INSTANCE_IP_KEY),
                        Integer.parseInt(ipAndPortParam.get(INSTANCE_PORT_KEY)));
                nacosServiceNameMap.remove(serviceName);
                break;
            default:
                break;
        }
    }
    
    private void registerAllInstances(TaskDO taskDO, NamingService destNamingService) throws Exception {
        CuratorFramework zk = zookeeperServerHolder.get(taskDO.getSourceClusterId());
        if (!ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName())) {
            registerALLInstances0(taskDO, destNamingService, zk, taskDO.getServiceName());
        } else {
            // 同步全部
            List<String> serviceList = zk.getChildren().forPath(DUBBO_ROOT_PATH);
            for (String serviceName : serviceList) {
                registerALLInstances0(taskDO, destNamingService, zk, serviceName);
            }
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
                destNamingService.batchRegisterInstance(
                        getServiceNameFromCache(taskDO.getTaskId(), serviceName, queryParam),
                        getGroupNameOrDefault(taskDO.getGroupName()), List.of(instance));
            }
        }
        
    }
    
    
    /**
     * cteate Dubbo service name
     *
     * @param taskId      taskId
     * @param serviceName dubbo metadata
     * @param queryParam  dubbo metadata
     */
    protected String getServiceNameFromCache(String taskId, String serviceName, Map<String, String> queryParam) {
        ConcurrentHashMap<String, String> partitionCache = nacosServiceNameMap.computeIfAbsent(taskId,
                (key) -> new ConcurrentHashMap<>());
        
        return partitionCache.computeIfAbsent(serviceName, (key) -> createServiceName(queryParam));
    }
    
}
