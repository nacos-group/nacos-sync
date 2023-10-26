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
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
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
import com.alibaba.nacossync.template.processor.TaskUpdateProcessor;
import com.alibaba.nacossync.timer.FastSyncHelper;
import com.alibaba.nacossync.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.nacossync.constant.SkyWalkerConstants.SOURCE_CLUSTERID_KEY;
import static com.alibaba.nacossync.util.NacosUtils.getGroupNameOrDefault;

/**
 * @author yangyshdan
 * @version $Id: ConfigServerSyncManagerService.java, v 0.1 2018-11-12 下午5:17 NacosSync Exp $$
 */

@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.NACOS)
public class NacosSyncToNacosServiceImpl implements SyncService, InitializingBean {
    
    private Map<String, EventListener> listenerMap = new ConcurrentHashMap<>();
    
    private final Map<String, Integer> syncTaskTap = new ConcurrentHashMap<>();
    
    @Autowired
    private MetricsManager metricsManager;
    
    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;
    
    @Autowired
    private NacosServerHolder nacosServerHolder;
    
    private ConcurrentHashMap<String, TaskDO> allSyncTaskMap = new ConcurrentHashMap<>();
    
    @Autowired
    private ClusterAccessService clusterAccessService;
    
    public static Map<String, Set<NamingService>> serviceClient = new ConcurrentHashMap<>();
    
    @Autowired
    private FastSyncHelper fastSyncHelper;
    
    @Autowired
    private TaskUpdateProcessor taskUpdateProcessor;
    
    /**
     * 因为网络故障等原因，nacos sync的同步任务会失败，导致目标集群注册中心缺少同步实例， 为避免目标集群注册中心长时间缺少同步实例，每隔5分钟启动一个兜底工作线程执行一遍全部的同步任务。
     */
    
    
    @Override
    public void afterPropertiesSet() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("com.alibaba.nacossync.basic.synctask");
            return t;
        });
        
        executorService.scheduleWithFixedDelay(() -> {
            if (allSyncTaskMap.size() == 0) {
                return;
            }
            
            try {
                Collection<TaskDO> taskCollections = allSyncTaskMap.values();
                List<TaskDO> taskDOList = new ArrayList<>(taskCollections);
                
                if (CollectionUtils.isNotEmpty(taskDOList)) {
                    fastSyncHelper.syncWithThread(taskDOList, this::timeSync);
                }
                
            } catch (Throwable e) {
                log.warn("basic synctask thread error", e);
            }
        }, 0, 300, TimeUnit.SECONDS);
    }
    
    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            NamingService sourceNamingService = nacosServerHolder.getSourceNamingService(taskDO.getTaskId(),
                    taskDO.getSourceClusterId());
            
            if ("ALL".equals(taskDO.getServiceName())) {
                String operationId = taskUpdateProcessor.getTaskIdAndOperationIdMap(taskDO.getTaskId());
                if (!StringUtils.isEmpty(operationId)) {
                    allSyncTaskMap.remove(operationId);
                }
                
                //处理group级别的服务任务删除
                ListView<String> servicesOfServer = sourceNamingService.getServicesOfServer(0, Integer.MAX_VALUE,
                        taskDO.getGroupName());
                List<String> serviceNames = servicesOfServer.getData();
                for (String serviceName : serviceNames) {
                    String operationKey = taskDO.getTaskId() + serviceName;
                    skyWalkerCacheServices.removeFinishedTask(operationKey);
                    allSyncTaskMap.remove(operationKey);
                    sourceNamingService.unsubscribe(serviceName, getGroupNameOrDefault(taskDO.getGroupName()),
                            listenerMap.remove(taskDO.getTaskId() + serviceName));
                    
                    List<Instance> sourceInstances = sourceNamingService.getAllInstances(serviceName,
                            getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), false);
                    List<Instance> deregisterInstances = new ArrayList<>();
                    for (Instance instance : sourceInstances) {
                        if (needSync(instance.getMetadata())) {
                            deregisterInstances.add(instance);
                        }
                    }
                    NamingService destNamingService = popNamingService(taskDO);
                    destNamingService.batchDeregisterInstance(serviceName,
                            getGroupNameOrDefault(taskDO.getGroupName()), deregisterInstances);
                }
            } else {
                //处理服务级别的任务删除
                String operationId = taskUpdateProcessor.getTaskIdAndOperationIdMap(taskDO.getTaskId());
                if (StringUtils.isEmpty(operationId)) {
                    log.warn("operationId is null data synchronization is not currently performed.{}", operationId);
                    return false;
                }
                
                sourceNamingService.unsubscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                        listenerMap.remove(operationId));
                List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(),
                        getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), false);
                
                List<Instance> deregisterInstances = new ArrayList<>();
                for (Instance instance : sourceInstances) {
                    if (needSync(instance.getMetadata())) {
                        deregisterInstances.add(instance);
                    }
                }
                NamingService destNamingService = popNamingService(taskDO);
                destNamingService.batchDeregisterInstance(taskDO.getServiceName(),
                        getGroupNameOrDefault(taskDO.getGroupName()), deregisterInstances);
                // 移除任务
                skyWalkerCacheServices.removeFinishedTask(operationId);
                // 移除所有需要同步的Task
                allSyncTaskMap.remove(operationId);
            }
        } catch (Exception e) {
            log.error("delete task from nacos to nacos was failed, operationalId:{}", taskDO.getOperationId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }
    
    @Override
    public boolean sync(TaskDO taskDO, Integer index) {
        log.info("线程 {} 开始同步 {} ", Thread.currentThread().getId(), System.currentTimeMillis());
        String operationId = taskDO.getOperationId();
        try {
            NamingService sourceNamingService = nacosServerHolder.getSourceNamingService(taskDO.getTaskId(),
                    taskDO.getSourceClusterId());
            NamingService destNamingService = getDestNamingService(taskDO, index);
            allSyncTaskMap.put(operationId, taskDO);
            //防止暂停同步任务后,重新同步/或删除任务以后新建任务不会再接收到新的事件导致不能同步,所以每次订阅事件之前,先全量同步一次任务
            long startTime = System.currentTimeMillis();
            doSync(operationId, taskDO, sourceNamingService, destNamingService);
            log.info("同步一个服务注册耗时:{} ms", System.currentTimeMillis() - startTime);
            this.listenerMap.putIfAbsent(operationId, event -> {
                if (event instanceof NamingEvent) {
                    NamingEvent namingEvent = (NamingEvent) event;
                    log.info("监听到服务{}信息改变, taskId：{}，实例数:{}，发起同步", namingEvent.getServiceName(),
                            operationId, namingEvent.getInstances() == null ? null : namingEvent.getInstances().size());
                    try {
                        doSync(operationId, taskDO, sourceNamingService, destNamingService);
                        log.info("监听到服务{}同步结束", namingEvent.getServiceName());
                    } catch (Exception e) {
                        log.error("event process fail, operationId:{}", operationId, e);
                        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                    }
                }
            });
            sourceNamingService.subscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    listenerMap.get(operationId));
        } catch (Exception e) {
            log.error("sync task from nacos to nacos was failed, operationId:{}", operationId, e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }
    
    /**
     * basic sync
     *
     * @param taskDO
     */
    public void timeSync(TaskDO taskDO) {
        log.debug("线程{}开始同步{}", Thread.currentThread().getId(), System.currentTimeMillis());
        String operationId = taskDO.getOperationId();
        try {
            NamingService sourceNamingService = nacosServerHolder.getSourceNamingService(taskDO.getTaskId(),
                    taskDO.getSourceClusterId());
            //获取目标集群client
            NamingService destNamingService = popNamingService(taskDO);
            long startTime = System.currentTimeMillis();
            doSync(operationId, taskDO, sourceNamingService, destNamingService);
            log.info("同步一个服务注册耗时:{} ms", System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private NamingService getDestNamingService(TaskDO taskDO, Integer index) {
        String key = taskDO.getSourceClusterId() + ":" + taskDO.getDestClusterId() + ":" + index;
        return nacosServerHolder.get(key);
    }
    
    private void doSync(String taskId, TaskDO taskDO, NamingService sourceNamingService,
            NamingService destNamingService) throws NacosException {
        if (syncTaskTap.putIfAbsent(taskId, 1) != null) {
            log.info("任务Id:{}上一个同步任务尚未结束", taskId);
            return;
        }
        //记录目标集群的Client
        recordNamingService(taskDO, destNamingService);
        try {
            
            List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(),
                    getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
            
            int level = clusterAccessService.findClusterLevel(taskDO.getSourceClusterId());
            if (CollectionUtils.isNotEmpty(sourceInstances) && sourceInstances.get(0).isEphemeral()) {
                //处临实例的批量数据同步,需要获取当前所有的服务实例，TODO，当Client为1.x的时候，执行和持久化实例一样的同步方式
                handlerPersistenceInstance(taskDO, destNamingService, sourceInstances, level);
            } else if (CollectionUtils.isEmpty(sourceInstances)) {
                //如果当前源集群是空的 ，那么直接注销目标集群的实例
                log.debug("service {} need sync Ephemeral instance num is null: serviceName ", taskDO.getServiceName());
                processDeRegisterInstances(taskDO, destNamingService);
            } else {
                //处临持久化实例的批量数据同步
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
            log.debug("检查实例: {}", instance);
            if (needSync(instance.getMetadata(), level, taskDO.getDestClusterId())) {
                needRegisterInstance.add(instance);
                log.debug("检查结果: 需注册");
            }
        }
        List<Instance> destAllInstances = destNamingService.getAllInstances(taskDO.getServiceName(),
                getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
        
        // 获取目标集群自己已经同步的实例
        List<Instance> destHasSyncInstances = destAllInstances.stream()
                .filter(instance -> hasSync(instance, taskDO.getSourceClusterId())).collect(Collectors.toList());
        
        //获取新增的实例，遍历新增
        List<Instance> newInstances = new ArrayList<>(needRegisterInstance);
        instanceRemove(destHasSyncInstances, newInstances);
        //构建同步实例列表
        List<Instance> newSyncInstances = new ArrayList<>(newInstances.size());
        for (Instance newInstance : newInstances) {
            Instance newSyncInstance = buildSyncInstance(newInstance, taskDO);
            log.debug("准备同步实例: {}", newSyncInstance);
            newSyncInstances.add(newSyncInstance);
        }
        //批量注册
        log.debug("准备批量注册: {}", taskDO);
        destNamingService.batchRegisterInstance(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                newSyncInstances);
        List<Instance> notRemoveInstances = new ArrayList<>();
        for (Instance destHasSyncInstance : destHasSyncInstances) {
            for (Instance instance : needRegisterInstance) {
                if (instanceEquals(destHasSyncInstance, instance)) {
                    notRemoveInstances.add(destHasSyncInstance);
                    log.debug("目标集群保留实例: {}", destHasSyncInstance);
                }
            }
        }
        destHasSyncInstances.removeAll(notRemoveInstances);
        
        if (CollectionUtils.isNotEmpty(destHasSyncInstances)) {
            log.info("taskid：{}，服务 {} 发生反注册，执行数量 {} ", taskDO.getTaskId(), taskDO.getServiceName(),
                    destHasSyncInstances.size());
            //批量反注册
            destNamingService.batchDeregisterInstance(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    destHasSyncInstances);
        }

    }
    
    
    public static boolean instanceEquals(Instance ins1, Instance ins2) {
        return (ins1.getIp().equals(ins2.getIp())) && (ins1.getPort() == ins2.getPort()) && (ins1.getWeight()
                == ins2.getWeight()) && (ins1.isHealthy() == ins2.isHealthy()) && (ins1.isEphemeral()
                == ins2.isEphemeral()) && (ins1.getClusterName().equals(ins2.getClusterName()))
                && (ins1.getServiceName().equals(ins2.getServiceName()));
    }
    
    private void instanceRemove(List<Instance> destHasSyncInstances, List<Instance> newInstances) {
        List<Instance> needRemoveInstance = new ArrayList<>();
        for (Instance destHasSyncInstance : destHasSyncInstances) {
            log.debug("检查目标集群实例: {}", destHasSyncInstance);
            for (Instance newInstance : newInstances) {
                if (destHasSyncInstance.equals(newInstance)) {
                    //如果目标集群已经存在了源集群同步过来的实例，就不需要同步了
                    needRemoveInstance.add(newInstance);
                    log.debug("检查结果: 不需同步");
                }
            }
        }
        // eg:A Cluster 已经同步到 B Cluster的实例数据，就不需要再重复同步过来了
        newInstances.removeAll(needRemoveInstance);
    }
    
    private boolean hasSync(Instance instance, String sourceClusterId) {
        if (instance.getMetadata() != null) {
            String sourceClusterKey = instance.getMetadata().get(SkyWalkerConstants.SOURCE_CLUSTERID_KEY);
            return sourceClusterKey != null && sourceClusterKey.equals(sourceClusterId);
        }
        return false;
    }
    
    
    /**
     * 当源集群需要同步的实例个数为0时,目标集群如果还有源集群同步的实例，执行反注册
     *
     * @param taskDO
     * @param destNamingService
     * @throws NacosException
     */
    private void processDeRegisterInstances(TaskDO taskDO, NamingService destNamingService) throws NacosException {
        //如果此时sourceInstance中的实例为空，证明此时实例下线或实例不存在
        List<Instance> destInstances = destNamingService.getAllInstances(taskDO.getServiceName(),
                getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), false);
        // 如果目标集群中的数据实例也为空了，则测试无需操作
        if (CollectionUtils.isEmpty(destInstances)) {
            return;
        }
        deRegisterFilter(destInstances, taskDO.getSourceClusterId());
        if (CollectionUtils.isNotEmpty(destInstances)) {
            //批量反注册
            destNamingService.batchDeregisterInstance(taskDO.getServiceName(),
                    getGroupNameOrDefault(taskDO.getGroupName()), destInstances);
        }
    }
    
    private void deRegisterFilter(List<Instance> destInstances, String sourceClusterId) {
        List<Instance> newDestInstance = new ArrayList<>();
        for (Instance destInstance : destInstances) {
            Map<String, String> metadata = destInstance.getMetadata();
            String destSourceClusterId = metadata.get(SkyWalkerConstants.SOURCE_CLUSTERID_KEY);
            if (needDeregister(destSourceClusterId, sourceClusterId)) {
                // 需要执行反注册
                newDestInstance.add(destInstance);
            }
        }
        destInstances = newDestInstance;
    }
    
    private boolean needDeregister(String destClusterId, String sourceClusterId) {
        if (!StringUtils.isEmpty(destClusterId)) {
            return destClusterId.equals(sourceClusterId);
        }
        return false;
    }
    
    private boolean needSync(Map<String, String> sourceMetaData, int level, String destClusterId) {
        //普通集群（默认）
        if (level == 0) {
            return SyncService.super.needSync(sourceMetaData);
        }
        //中心集群，只要不是目标集群传过来的实例，都需要同步（扩展功能）
        if (!destClusterId.equals(sourceMetaData.get(SOURCE_CLUSTERID_KEY))) {
            return true;
        }
        return false;
    }
    
    private void recordNamingService(TaskDO taskDO, NamingService destNamingService) {
        String key = buildClientKey(taskDO);
        serviceClient.computeIfAbsent(key, clientKey -> {
            Set<NamingService> hashSet = new ConcurrentHashSet<>();
            hashSet.add(destNamingService);
            return hashSet;
        });
    }
    
    public NamingService popNamingService(TaskDO taskDO) {
        String key = buildClientKey(taskDO);
        Set<NamingService> namingServices = serviceClient.get(key);
        if (CollectionUtils.isNotEmpty(namingServices)) {
            return namingServices.iterator().next();
        }
        log.warn("{} 无可用 namingservice", key);
        return null;
    }
    
    private static String buildClientKey(TaskDO taskDO) {
        return taskDO.getId() + ":" + taskDO.getServiceName();
    }
    
    private Instance buildSyncInstance(Instance instance, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(instance.getIp());
        temp.setPort(instance.getPort());
        temp.setClusterName(instance.getClusterName());
        //instance的serviceName含组名前缀，但Nacos2服务端检查serviceName参数时不能包含组名前缀，故不再设置serviceName。
        temp.setEnabled(instance.isEnabled());
        temp.setHealthy(instance.isHealthy());
        temp.setWeight(instance.getWeight());
        temp.setEphemeral(instance.isEphemeral());
        Map<String, String> metaData = new HashMap<>(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        //标识是同步实例
        metaData.put(SkyWalkerConstants.SYNC_INSTANCE_TAG, taskDO.getSourceClusterId() + "@@" + taskDO.getVersion());
        temp.setMetadata(metaData);
        return temp;
    }
    
    
}
