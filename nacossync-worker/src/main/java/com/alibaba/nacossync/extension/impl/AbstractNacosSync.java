package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.BatchTaskExecutor;
import com.google.common.base.Stopwatch;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.nacossync.util.NacosUtils.getGroupNameOrDefault;

@Slf4j
public abstract class AbstractNacosSync implements SyncService {
    
    private final Map<String, EventListener> listenerMap = new ConcurrentHashMap<>();
    
    private final Map<String, Set<String>> sourceInstanceSnapshot = new ConcurrentHashMap<>();
    
    private final Map<String, Integer> syncTaskTap = new ConcurrentHashMap<>();
    
    private final ConcurrentHashMap<String, TaskDO> allSyncTaskMap = new ConcurrentHashMap<>();
    private ScheduledExecutorService executorService;
    @Autowired
    private MetricsManager metricsManager;
    
    @Getter
    @Autowired
    private NacosServerHolder nacosServerHolder;
    
    
    /**
     * Due to network issues or other reasons, the Nacos Sync synchronization tasks may fail,
     * resulting in the target cluster's registry missing synchronized instances.
     * To prevent the target cluster's registry from missing synchronized instances for an extended period,
     * a fallback worker thread is started every 5 minutes to execute all synchronization tasks.
     */
    @PostConstruct
    public void afterPropertiesSet() {
        initializeExecutorService();
        scheduleSyncTasks();
    }
    
    @PreDestroy
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
            doSync(taskId, task, sourceNamingService);
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
        String taskId = taskDO.getTaskId();
        try {
            
            NamingService sourceNamingService = nacosServerHolder.get(taskDO.getSourceClusterId());
            //移除订阅
            EventListener listener = listenerMap.remove(taskId);
            if (listener!= null) {
                sourceNamingService.unsubscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()), listener);
            }
            sourceNamingService.unsubscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    listenerMap.remove(taskId));
            sourceInstanceSnapshot.remove(taskId);
            allSyncTaskMap.remove(taskId);
            
            // 删除目标集群中同步的实例列表
            deregisterInstance(taskDO);
        }catch (NacosException e) {
            log.error("Delete task from nacos to specify destination was failed, taskId:{}", taskId, e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        } catch (Exception e) {
            log.error("Unexpected error during sync task, taskId:{}", taskId, e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
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
            sourceNamingService.subscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    listenerMap.get(taskId));
        }catch (NacosException e) {
            log.error("Nacos sync task process fail, taskId:{}", taskId, e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        } catch (Exception e) {
            log.error("Unexpected error during sync task, taskId:{}", taskId, e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }
    
    private void doSync(String taskId, TaskDO taskDO, NamingService sourceNamingService) throws Exception {
        if (syncTaskTap.putIfAbsent(taskId, 1) != null) {
            log.info("任务Id:{}上一个同步任务尚未结束", taskId);
            return;
        }
        try {
            // 直接从本地保存的serviceInfoMap中取订阅的服务实例
            List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(),
                    getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
            // 先删除不存在的
            this.removeInvalidInstance(taskDO, sourceInstances);
            
            // 同步实例
            this.syncNewInstance(taskDO, sourceInstances);
        } finally {
            syncTaskTap.remove(taskId);
        }
    }
    
    private void syncNewInstance(TaskDO taskDO, List<Instance> sourceInstances) throws NacosException {
        Set<String> latestSyncInstance = new TreeSet<>();
        //再次添加新实例
        String taskId = taskDO.getTaskId();
        Set<String> instanceKeys = sourceInstanceSnapshot.get(taskId);
        for (Instance instance : sourceInstances) {
            if (needSync(instance.getMetadata())) {
                String instanceKey = composeInstanceKey(instance.getIp(), instance.getPort());
                if (CollectionUtils.isEmpty(instanceKeys) || !instanceKeys.contains(instanceKey)) {
                    register(taskDO, instance);
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
    
    public abstract void register(TaskDO taskDO, Instance instance);
    
    public abstract void deregisterInstance(TaskDO taskDO) throws Exception;
    
    public abstract void removeInvalidInstance(TaskDO taskDO, Set<String> invalidInstanceKeys) throws Exception;
    
}
