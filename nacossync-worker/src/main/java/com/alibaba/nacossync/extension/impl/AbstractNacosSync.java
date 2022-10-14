package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.NacosUtils;
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
     * Due to network failures and other reasons, the synchronization task of nacos sync will fail,
     * resulting in the lack of synchronization instances in the target cluster registry.
     * In order to avoid the lack of synchronization instances in the target cluster registry for a long time,
     * a bottom worker thread is started every 5 minutes to execute all synchronization tasks.
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
            //remove subscribe
            sourceNamingService.unsubscribe(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    listenerMap.remove(taskDO.getTaskId()));
            sourceInstanceSnapshot.remove(taskDO.getTaskId());
            allSyncTaskMap.remove(taskDO.getTaskId());
            
            //Delete the synchronized instance list in the target cluster
            deregisterInstance(taskDO);
        } catch (Exception e) {
            log.error("delete task from nacos to specify destination was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }
    
    @Override
    public boolean sync(TaskDO taskDO) {
        String taskId = taskDO.getTaskId();
        try {
            NamingService sourceNamingService = nacosServerHolder.get(taskDO.getSourceClusterId());
            allSyncTaskMap.put(taskId, taskDO);
            /**
             * After the synchronization task is suspended, the newly created task will not receive new events after the task is re-synchronized/or deleted,
             * so that the synchronization cannot be performed. Therefore, before each subscription event, first synchronize the full amount of tasks once.
             */
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
        } catch (Exception e) {
            log.error("sync task from nacos to specify destination was failed, taskId:{}", taskId, e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }
    
    private void doSync(String taskId, TaskDO taskDO, NamingService sourceNamingService) throws NacosException {
        if (syncTaskTap.putIfAbsent(taskId, 1) != null) {
            log.info("TaskId:{}, The last synchronization task has not ended", taskId);
            return;
        }

        try {
            String serviceName = taskDO.getServiceName();
            if (serviceName.equals("*") || "".equals(serviceName)) {
                ListView<String> servers = sourceNamingService.getServicesOfServer(1, Integer.MAX_VALUE, getGroupNameOrDefault(taskDO.getGroupName()));
                if (servers != null) {
                    List<String> services = servers.getData();
                    if (services != null && !services.isEmpty()) {
                        for (String syncServiceName : services) {
                            taskDO.setServiceName(syncServiceName);
                            List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(),
                                    getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
                            this.removeInvalidInstance(taskDO, sourceInstances);
                            this.syncNewInstance(taskDO, sourceInstances);

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
                        }
                    }
                }
            } else {

                List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName(),
                        getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
                this.removeInvalidInstance(taskDO, sourceInstances);
                this.syncNewInstance(taskDO, sourceInstances);

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
            }
        } finally {
            syncTaskTap.remove(taskId);
        }
    }
    
    private void syncNewInstance(TaskDO taskDO, List<Instance> sourceInstances) throws NacosException {
        Set<String> latestSyncInstance = new TreeSet<>();
        String taskId = taskDO.getTaskId();
        Set<String> instanceKeys = sourceInstanceSnapshot.get(taskId);
        for (Instance instance : sourceInstances) {
            String instanceKey = composeInstanceKey(instance.getIp(), instance.getPort());
            if (CollectionUtils.isEmpty(instanceKeys) || !instanceKeys.contains(instanceKey)) {
                register(taskDO, instance);
            }
            latestSyncInstance.add(instanceKey);
        }
        
        if (CollectionUtils.isNotEmpty(latestSyncInstance)) {
            log.info("Task Id: {}, Number of synchronized instances: {}", taskId, latestSyncInstance.size());
            sourceInstanceSnapshot.put(taskId, latestSyncInstance);
        } else {
            sourceInstanceSnapshot.remove(taskId);
        }
    }
    
    
    private void removeInvalidInstance(TaskDO taskDO, List<Instance> sourceInstances) throws NacosException {
        String taskId = taskDO.getTaskId();
        if (this.sourceInstanceSnapshot.containsKey(taskId)) {
            Set<String> oldInstanceKeys = this.sourceInstanceSnapshot.get(taskId);
            Set<String> newInstanceKeys = sourceInstances.stream()
                    .map(instance -> composeInstanceKey(instance.getIp(), instance.getPort()))
                    .collect(Collectors.toSet());
            oldInstanceKeys.removeAll(newInstanceKeys);
            if (CollectionUtils.isNotEmpty(oldInstanceKeys)) {
                log.info("Task Id: {}, remove invalid synchronization instance: {}", taskId, oldInstanceKeys);
                removeInvalidInstance(taskDO, oldInstanceKeys);
            }
        }
    }
    
    public abstract String composeInstanceKey(String ip, int port);
    
    public abstract void register(TaskDO taskDO, Instance instance);
    
    public abstract void deregisterInstance(TaskDO taskDO) throws Exception;
    
    public abstract void removeInvalidInstance(TaskDO taskDO, Set<String> invalidInstanceKeys);
    
    public NacosServerHolder getNacosServerHolder() {
        return nacosServerHolder;
    }
}
