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
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.MapUtils;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.BatchTaskExecutor;
import com.alibaba.nacossync.util.StringUtils;
import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.nacossync.constant.SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY;
import static com.alibaba.nacossync.util.NacosUtils.getGroupNameOrDefault;

/**
 * @author yangyshdan
 * @version $Id: ConfigServerSyncManagerService.java, v 0.1 2018-11-12 下午5:17 NacosSync Exp $$
 */

@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.NACOS)
public class NacosSyncToNacosServiceImpl implements SyncService, InitializingBean, DisposableBean {
    
    private final Map<String, EventListener> listenerMap = new ConcurrentHashMap<>();
    
    private final Map<String, Integer> syncTaskTap = new ConcurrentHashMap<>();
    
    private final ConcurrentHashMap<String, TaskDO> allSyncTaskMap = new ConcurrentHashMap<>();
    
    private ScheduledExecutorService executorService;
    
    private final MetricsManager metricsManager;
    
    private final NacosServerHolder nacosServerHolder;
    
    private final ClusterAccessService clusterAccessService;
    
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    
    public NacosSyncToNacosServiceImpl(MetricsManager metricsManager, NacosServerHolder nacosServerHolder,
            ClusterAccessService clusterAccessService, SkyWalkerCacheServices skyWalkerCacheServices) {
        this.metricsManager = metricsManager;
        this.nacosServerHolder = nacosServerHolder;
        this.clusterAccessService = clusterAccessService;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
    }
    
    /**
     * Due to network issues or other reasons, the Nacos Sync synchronization tasks may fail,
     * resulting in the target cluster's registry missing synchronized instances.
     * To prevent the target cluster's registry from missing synchronized instances for an extended period,
     * a fallback worker thread is started every 5 minutes to execute all synchronization tasks.
     */
    @Override
    public void afterPropertiesSet() {
        initializeExecutorService();
        scheduleSyncTasks();
    }
    
    @Override
    public void destroy() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    
    private void initializeExecutorService() {
        executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("com.alibaba.nacossync.basic.synctask");
            return t;
        });
    }
    
    private void scheduleSyncTasks() {
        executorService.scheduleWithFixedDelay(this::executeSyncTasks, 0, 300, TimeUnit.SECONDS);
    }
    
    private void executeSyncTasks() {
        if (allSyncTaskMap.isEmpty()) {
            return;
        }
        
        Collection<TaskDO> taskCollections = allSyncTaskMap.values();
        List<TaskDO> taskDOList = new ArrayList<>(taskCollections);
        
        if (CollectionUtils.isNotEmpty(taskDOList)) {
            BatchTaskExecutor.batchOperation(taskDOList, this::executeTask);
        }
    }
    
    private void executeTask(TaskDO task) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String taskId = task.getTaskId();
        try {
            NamingService sourceNamingService = nacosServerHolder.get(task.getSourceClusterId());
            NamingService destNamingService = nacosServerHolder.get(task.getDestClusterId());
            doSync(taskId, task, sourceNamingService, destNamingService);
        } catch (NacosException e) {
            log.error("sync task from nacos to nacos failed, taskId:{}", taskId, e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
        } catch (Exception e) {
            log.error("Unexpected error during sync task, taskId:{}", taskId, e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
        } finally {
            stopwatch.stop();
            log.debug("Task execution time for taskId {}: {} ms", taskId, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }
    
    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            NamingService sourceNamingService = nacosServerHolder.get(taskDO.getSourceClusterId());
            int level = clusterAccessService.findClusterLevel(taskDO.getSourceClusterId());
            String taskId = taskDO.getTaskId();
            
            //Handle individual service
            if (StringUtils.isEmpty(taskId)) {
                log.warn("taskId is null data synchronization is not currently performed.{}", taskId);
                return false;
            }
            EventListener listener = listenerMap.remove(taskId);
            if (listener!= null) {
                sourceNamingService.unsubscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()), listener);
            }
            List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(),
                    getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), false);
            
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());
            for (Instance instance : sourceInstances) {
                if (needSync(instance.getMetadata(), level, taskDO.getDestClusterId())) {
                    destNamingService.deregisterInstance(taskDO.getServiceName(),
                            getGroupNameOrDefault(taskDO.getGroupName()), instance);
                }
            }
            // Remove all tasks that need to be synchronized.
            allSyncTaskMap.remove(taskId);
            
        } catch (Exception e) {
            log.error("delete task from nacos to nacos was failed, operationalId:{}", taskDO.getOperationId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }
    
    @Override
    public boolean sync(TaskDO taskDO, Integer index) {
        log.info("Thread {} started synchronization at {}", Thread.currentThread().getId(), System.currentTimeMillis());
        String taskId = taskDO.getTaskId();
        try {
            NamingService sourceNamingService = nacosServerHolder.get(taskDO.getSourceClusterId());
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());
            allSyncTaskMap.put(taskId, taskDO);
            // To prevent issues where tasks paused for synchronization, newly created tasks after deletion,
            // or resynchronization tasks do not receive new events and hence cannot synchronize,
            // perform a full synchronization of tasks before subscribing to events each time.
            Stopwatch stopwatch = Stopwatch.createStarted();
            doSync(taskId, taskDO, sourceNamingService, destNamingService);
            log.debug("Time taken to synchronize a service registration: {} ms",
                    stopwatch.elapsed(TimeUnit.MILLISECONDS));
            this.listenerMap.putIfAbsent(taskId, event -> {
                if (event instanceof NamingEvent) {
                    NamingEvent namingEvent = (NamingEvent) event;
                    log.info("Detected changes in service {} information, taskId: {}, number of instances: {}, initiating synchronization",
                            namingEvent.getServiceName(), taskId,
                            namingEvent.getInstances() == null ? null : namingEvent.getInstances().size());
                    try {
                        doSync(taskId, taskDO, sourceNamingService, destNamingService);
                        log.info("Detected synchronization end for service {}", namingEvent.getServiceName());
                    } catch (Exception e) {
                        log.error("event process fail, taskId:{}", taskId, e);
                        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                    }
                }
            });
            sourceNamingService.subscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    listenerMap.get(taskId));
        } catch (Exception e) {
            log.error("sync task from nacos to nacos was failed, taskId:{}", taskId, e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }
    
    
    private void doSync(String taskId, TaskDO taskDO, NamingService sourceNamingService,
            NamingService destNamingService) throws NacosException {
        if (syncTaskTap.putIfAbsent(taskId, 1) != null) {
            log.info("Task ID:{} - the previous synchronization task has not finished yet", taskId);
            return;
        }
        
        try {
            
            List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(),
                    getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
            
            int level = clusterAccessService.findClusterLevel(taskDO.getSourceClusterId());
            if (CollectionUtils.isNotEmpty(sourceInstances) && sourceInstances.get(0).isEphemeral()) {
                // Handle batch data synchronization of ephemeral instances, need to get all current service instances.
                // TODO: When the Client is version 1.x, execute the same synchronization method as persistent instances.
                handlerPersistenceInstance(taskDO, destNamingService, sourceInstances, level);
            } else if (CollectionUtils.isEmpty(sourceInstances)) {
                // If the current source cluster is empty, then directly deregister the instances in the target cluster.
                log.debug("service {} needs to sync ephemeral instance num is null: serviceName ", taskDO.getServiceName());
                processDeRegisterInstances(taskDO, destNamingService);
            } else {
                // Handle batch data synchronization of persistent instances.
                handlerPersistenceInstance(taskDO, destNamingService, sourceInstances, level);
            }
        } finally {
            syncTaskTap.remove(taskId);
        }
    }
    
    private void handlerPersistenceInstance(TaskDO taskDO, NamingService destNamingService,
            List<Instance> sourceInstances, int level) throws NacosException {
        List<Instance> needRegisterInstance = new ArrayList<>();
        for (Instance instance : sourceInstances) {
            if (needSync(instance.getMetadata(), level, taskDO.getDestClusterId())) {
                needRegisterInstance.add(buildSyncInstance(instance, taskDO));
            }
        }
        List<Instance> destAllInstances = destNamingService.getAllInstances(taskDO.getServiceName(),
                getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
        
        // Get the instances that the destination cluster has already synchronized
        List<Instance> destHasSyncInstances = destAllInstances.stream()
                .filter(instance -> hasSync(instance, taskDO.getSourceClusterId())).collect(Collectors.toList());
        
        // The following two conversions are necessary because the Nacos Instance's equals method
        // is flawed and cannot be used for direct comparison.
        // The reason is that Instance's equals method compares Metadata using the toString method,
        // and Metadata is of type HashMap, which does not guarantee order in its toString representation.
        
        // Convert destHasSyncInstances to a Map with the concatenated string as key
        Map<String, Instance> destInstanceMap = destHasSyncInstances.stream()
                .collect(Collectors.toMap(NacosSyncToNacosServiceImpl::getInstanceKey, instance -> instance));
        
        // Convert newInstances to a Map with the concatenated string as key
        Map<String, Instance> needRegisterMap = needRegisterInstance.stream()
                .collect(Collectors.toMap(NacosSyncToNacosServiceImpl::getInstanceKey, instance -> instance));
        
        // Remove instances from newInstanceMap that are present in destInstanceMap
        List<Instance> newInstances = removeSyncedInstances(destInstanceMap, needRegisterMap);
        
        // Remove instances from destInstanceMap that are present in newInstanceMap
        List<Instance> invalidInstances = getInvalidInstances(destInstanceMap, needRegisterMap);
        
        // Register each instance one by one. Take one instance at a time.
        for (Instance newInstance : newInstances) {
            destNamingService.registerInstance(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    newInstance);
        }
        
        if (CollectionUtils.isNotEmpty(invalidInstances)) {
            log.info("taskId: {}, service {} deregistered, number of executions: {}", taskDO.getTaskId(), taskDO.getServiceName(),
                    destHasSyncInstances.size());
        }
        for (Instance instance : invalidInstances) {
            destNamingService.deregisterInstance(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    instance);
        }
    }
    
    private List<Instance> getInvalidInstances(Map<String, Instance> destInstanceMap, Map<String, Instance> needRegisterMap) {
        Map<String, Instance> destClone = new HashMap<>(destInstanceMap);
        
        // Convert newInstances to a Map with the concatenated string as key
        Map<String, Instance> needRegisterClone = new HashMap<>(needRegisterMap);
        
        // Remove instances from newInstanceMap that are present in destInstanceMap
        destClone.keySet().removeAll(needRegisterClone.keySet());
        
        return new ArrayList<>(destClone.values());
    }
    
    
    public List<Instance> removeSyncedInstances(Map<String, Instance> destInstanceMap, Map<String, Instance> needRegisterMap) {
        List<Instance> result = new ArrayList<>();
        for (Map.Entry<String, Instance> entry : needRegisterMap.entrySet()) {
            String key = entry.getKey();
            Instance needRegisterInstance = entry.getValue();
            // If the key does not exist in the destination cluster, it needs to be returned
            if (!destInstanceMap.containsKey(key)) {
                result.add(needRegisterInstance);
            } else {
                // If the key exists, check if the metadata is the same
                Instance destInstance = destInstanceMap.get(key);
                if (!isMetadataEqual(needRegisterInstance.getMetadata(), destInstance.getMetadata())) {
                    result.add(needRegisterInstance);
                }
            }
        }
        
        return result;
    }
    
    private boolean isMetadataEqual(Map<String, String> sourceMetadata, Map<String, String> destMetadata) {
        if (MapUtils.isEmpty(sourceMetadata)) {
            return MapUtils.isEmpty(destMetadata);
        }
        return sourceMetadata.equals(destMetadata);
    }
   
    
    private boolean hasSync(Instance instance, String sourceClusterId) {
        if (instance.getMetadata() != null) {
            String sourceClusterKey = instance.getMetadata().get(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY);
            return sourceClusterKey != null && sourceClusterKey.equals(sourceClusterId);
        }
        return false;
    }
    
    
    /**
     * When the number of instances that the source cluster needs to synchronize is 0,
     * if the target cluster still has instances synchronized with the source cluster,
     * perform unregistration.
     *
     * @param destNamingService Destination cluster naming service
     * @throws NacosException
     */
    private void processDeRegisterInstances(TaskDO taskDO, NamingService destNamingService) throws NacosException {
        // If the instances in sourceInstances are empty, it means the instances are offline or do not exist.
        List<Instance> destInstances = destNamingService.getAllInstances(taskDO.getServiceName(),
                getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), false);
        // If the instances in the target cluster are also empty, then no operation is needed.
        if (CollectionUtils.isEmpty(destInstances)) {
            return;
        }
        destInstances = filterInstancesForRemoval(destInstances, taskDO.getSourceClusterId());
        if (CollectionUtils.isNotEmpty(destInstances)) {
            // Deregister each instance one by one. Take one instance at a time.
            // Need to handle redo, otherwise, it will be registered again.
            for (Instance destInstance : destInstances) {
                destNamingService.deregisterInstance(taskDO.getServiceName(),
                        getGroupNameOrDefault(taskDO.getGroupName()), destInstance);
            }
        }
    }
    
    private List<Instance> filterInstancesForRemoval(List<Instance> destInstances, String sourceClusterId) {
        return destInstances.stream().filter(instance -> !instance.getMetadata().isEmpty())
                .filter(instance -> needDeregister(instance.getMetadata().get(SOURCE_CLUSTER_ID_KEY), sourceClusterId))
                .collect(Collectors.toList());
        
    }
    
    private boolean needDeregister(String destClusterId, String sourceClusterId) {
        if (!StringUtils.isEmpty(destClusterId)) {
            return destClusterId.equals(sourceClusterId);
        }
        return false;
    }
    
    private boolean needSync(Map<String, String> sourceMetaData, int level, String destClusterId) {
        // Regular cluster (default)
        if (level == 0) {
            return SyncService.super.needSync(sourceMetaData);
        }
        // Central cluster, as long as the instance is not from the target cluster,
        // it needs to be synchronized (extended functionality)
        return !destClusterId.equals(sourceMetaData.get(SOURCE_CLUSTER_ID_KEY));
    }
    
    
    private Instance buildSyncInstance(Instance instance, TaskDO taskDO) {
        Instance temp = getInstance(instance);
        temp.addMetadata(SkyWalkerConstants.DEST_CLUSTER_ID_KEY, taskDO.getDestClusterId());
        temp.addMetadata(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        temp.addMetadata(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, taskDO.getSourceClusterId());
        //The flag is a synchronous instance
        temp.addMetadata(SkyWalkerConstants.SYNC_INSTANCE_TAG,
                taskDO.getSourceClusterId() + "@@" + taskDO.getVersion());
        return temp;
    }
    
    private static Instance getInstance(Instance instance) {
        Instance temp = new Instance();
        temp.setInstanceId(instance.getInstanceId());
        temp.setIp(instance.getIp());
        temp.setPort(instance.getPort());
        temp.setClusterName(instance.getClusterName());
        temp.setServiceName(instance.getServiceName());
        temp.setEnabled(instance.isEnabled());
        temp.setHealthy(instance.isHealthy());
        temp.setWeight(instance.getWeight());
        temp.setEphemeral(instance.isEphemeral());
        Map<String, String> metaData = new HashMap<>(instance.getMetadata());
        temp.setMetadata(metaData);
        return temp;
    }
    
    private static String getInstanceKey(Instance instance) {
        return String.join("|",
                instance.getIp(),
                String.valueOf(instance.getPort()),
                String.valueOf(instance.getWeight()),
                String.valueOf(instance.isHealthy()),
                String.valueOf(instance.isEphemeral()),
                instance.getClusterName(),
                instance.getServiceName());
    }
}
