package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.client.naming.NacosNamingService;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.NacosUtils;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.Application;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.nacossync.util.DubboConstants.ALL_SERVICE_NAME_PATTERN;
import static com.alibaba.nacossync.util.NacosUtils.getGroupNameOrDefault;

@Slf4j
public abstract class AbstractNacosSync implements SyncService {
    
    private final Map<String, EventListener> listenerMap = new ConcurrentHashMap<>();
    
    private final Map<String, Set<String>> sourceInstanceSnapshot = new ConcurrentHashMap<>();
    
    private final Map<String, Integer> syncTaskTap = new ConcurrentHashMap<>();
    
    private final ConcurrentHashMap<String, TaskDO> allSyncTaskMap = new ConcurrentHashMap<>();
    
    @Autowired
    private MetricsManager metricsManager;
    
    @Autowired
    private NacosServerHolder nacosServerHolder;
    
    
    /**
     * 因为网络故障等原因，nacos sync的同步任务会失败，导致目标集群注册中心缺少同步实例， 为避免目标集群注册中心长时间缺少同步实例，每隔5分钟启动一个兜底工作线程执行一遍全部的同步任务。
     */
    @PostConstruct
    public void startBasicSyncTaskThread() {
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
                for (TaskDO taskDO : allSyncTaskMap.values()) {
                    String taskId = taskDO.getTaskId();
                    NamingService sourceNamingService = nacosServerHolder.get(taskDO.getSourceClusterId());
                    try {
                        doSync(taskId, taskDO, sourceNamingService);
                    } catch (Exception e) {
                        log.error("basic sync task process fail, taskId:{}", taskId, e);
                        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                    }
                }
            } catch (Throwable e) {
                log.warn("basic synctask thread error", e);
            }
        }, 0, 300, TimeUnit.SECONDS);
    }
    
    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            NamingService sourceNamingService = nacosServerHolder.get(taskDO.getSourceClusterId());
            //移除订阅
            sourceNamingService.unsubscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    listenerMap.remove(taskDO.getTaskId()));
            sourceInstanceSnapshot.remove(taskDO.getTaskId());
            allSyncTaskMap.remove(taskDO.getTaskId());
            
            // 删除目标集群中同步的实例列表
            deregisterInstance(taskDO);
        } catch (Exception e) {
            log.error("delete task from nacos to specify destination was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }
    
    @Override
    public boolean sync(TaskDO taskDO, Integer index) {
        String taskId = taskDO.getTaskId();
        try {
            NamingService sourceNamingService = nacosServerHolder.get(taskDO.getSourceClusterId());
            allSyncTaskMap.put(taskId, taskDO);
            //防止暂停同步任务后,重新同步/或删除任务以后新建任务不会再接收到新的事件导致不能同步,所以每次订阅事件之前,先全量同步一次任务
            doSync(taskId, taskDO, sourceNamingService);
            this.listenerMap.putIfAbsent(taskId, event -> {
                if (event instanceof NamingEvent) {
                    try {
                        doSync(taskId, taskDO, sourceNamingService);
                    } catch (Exception e) {
                        log.error("event process fail, taskId:{}", taskId, e);
                        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                    }
                }
            });
            subscribeAllService(taskDO);
        } catch (Exception e) {
            log.error("sync task from nacos to specify destination was failed, taskId:{}", taskId, e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private void subscribeAllService(TaskDO taskDO) throws Exception {
        NamingService namingService = nacosServerHolder.get(taskDO.getSourceClusterId());
        if (!ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName())) {
            namingService.subscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    listenerMap.get(taskDO.getTaskId()));
        } else {
            // 订阅全部
            List<String> serviceList = namingService.getServicesOfServer(0, Integer.MAX_VALUE, taskDO.getGroupName()).getData();
            for (String serviceName : serviceList) {
                namingService.subscribe(serviceName, getGroupNameOrDefault(taskDO.getGroupName()),
                        listenerMap.get(taskDO.getTaskId()));
            }
        }
    }
    
    private void doSync(String taskId, TaskDO taskDO, NamingService sourceNamingService) throws Exception {
        if (syncTaskTap.putIfAbsent(taskId, 1) != null) {
            log.info("任务Id:{}上一个同步任务尚未结束", taskId);
            return;
        }
        try {
            registerAllInstances(taskDO);
        } finally {
            syncTaskTap.remove(taskId);
        }
    }

    private void registerAllInstances(TaskDO taskDO) throws Exception {
        NamingService namingService = nacosServerHolder.get(taskDO.getSourceClusterId());
        if (!ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName())) {
            registerALLInstances0(taskDO, namingService, taskDO.getServiceName());
        } else {
            // 同步全部
            List<String> serviceList = namingService.getServicesOfServer(0, Integer.MAX_VALUE, taskDO.getGroupName()).getData();
            for (String serviceName : serviceList) {
                registerALLInstances0(taskDO, namingService, serviceName);
            }
        }
    }

    private void registerALLInstances0(TaskDO taskDO, NamingService sourceNamingService, String serviceName) throws Exception {
        // 直接从本地保存的serviceInfoMap中取订阅的服务实例
        List<Instance> sourceInstances = sourceNamingService.getAllInstances(serviceName,
                getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
        // 先删除不存在的
        this.removeInvalidInstance(taskDO, sourceInstances);

        // 同步实例
        this.syncNewInstance(taskDO, sourceInstances, serviceName);
    }
    
    private void syncNewInstance(TaskDO taskDO, List<Instance> sourceInstances, String serviceName) throws NacosException {
        Set<String> latestSyncInstance = new TreeSet<>();
        //再次添加新实例
        String taskId = taskDO.getTaskId();
        Set<String> instanceKeys = sourceInstanceSnapshot.get(taskId);
        for (Instance instance : sourceInstances) {
            if (needSync(instance.getMetadata())) {
                String instanceKey = composeInstanceKey(instance.getIp(), instance.getPort());
                if (CollectionUtils.isEmpty(instanceKeys) || !instanceKeys.contains(instanceKey)) {
                    register(taskDO, instance, serviceName);
                }
                latestSyncInstance.add(instanceKey);
            }
        }
        
        if (CollectionUtils.isNotEmpty(latestSyncInstance)) {
            log.info("任务Id:{},已同步实例个数:{}", taskId, latestSyncInstance.size());
            sourceInstanceSnapshot.put(taskId, latestSyncInstance);
        } else {
            // latestSyncInstance为空表示源集群中需要同步的所有实例（即非nacos-sync同步过来的实例）已经下线,清除本地持有快照
            sourceInstanceSnapshot.remove(taskId);
        }
    }
    
    
    private void removeInvalidInstance(TaskDO taskDO, List<Instance> sourceInstances) throws Exception {
        String taskId = taskDO.getTaskId();
        if (this.sourceInstanceSnapshot.containsKey(taskId)) {
            Set<String> oldInstanceKeys = this.sourceInstanceSnapshot.get(taskId);
            Set<String> newInstanceKeys = sourceInstances.stream()
                    .map(instance -> composeInstanceKey(instance.getIp(), instance.getPort()))
                    .collect(Collectors.toSet());
            oldInstanceKeys.removeAll(newInstanceKeys);
            if (CollectionUtils.isNotEmpty(oldInstanceKeys)) {
                log.info("任务Id:{},移除无效同步实例:{}", taskId, oldInstanceKeys);
                removeInvalidInstance(taskDO, oldInstanceKeys);
            }
        }
    }
    
    @Override
    public boolean needDelete(Map<String, String> destMetaData, TaskDO taskDO) {
        return SyncService.super.needDelete(destMetaData, taskDO);
    }
    
    @Override
    public boolean needSync(Map<String, String> sourceMetaData) {
        return SyncService.super.needSync(sourceMetaData);
    }
    
    public abstract String composeInstanceKey(String ip, int port);
    
    public abstract void register(TaskDO taskDO, Instance instance, String serviceName);
    
    public abstract void deregisterInstance(TaskDO taskDO) throws Exception;
    
    public abstract void removeInvalidInstance(TaskDO taskDO, Set<String> invalidInstanceKeys) throws Exception;
    
    public NacosServerHolder getNacosServerHolder() {
        return nacosServerHolder;
    }
}
