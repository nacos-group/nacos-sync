/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacossync.timer;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.event.DeleteAllSubTaskEvent;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.event.SyncTaskEvent;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.BatchTaskExecutor;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import com.google.common.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * when the database task service name is empty, check all the services in the cluster and create a synchronization
 * task.
 */
@Slf4j
public class CheckRunningStatusAllNacosThread implements Runnable {
    
    private final Map<String, Set<String>> subTaskService = new ConcurrentHashMap<>();
    
    private final MetricsManager metricsManager;
    
    private final TaskAccessService taskAccessService;
    
    private final NacosServerHolder nacosServerHolder;
    
    private final EventBus eventBus;
    
    
    public CheckRunningStatusAllNacosThread(MetricsManager metricsManager, TaskAccessService taskAccessService,
            NacosServerHolder nacosServerHolder, EventBus eventBus) {
        this.metricsManager = metricsManager;
        this.taskAccessService = taskAccessService;
        this.nacosServerHolder = nacosServerHolder;
        this.eventBus = eventBus;
    }
    
    /**
     * Synchronize data based on the ns level.
     */
    @Override
    public void run() {
        
        try {
            List<TaskDO> tasks = taskAccessService.findAllByServiceNameEqualAll();
            if (CollectionUtils.isEmpty(tasks)) {
                return;
            }
            
            // Get the set of all task IDs
            Set<String> taskIdSet = tasks.stream().map(TaskDO::getTaskId).collect(Collectors.toSet());
            
            // Filter and handle sub-tasks that need to be deleted. This handles the case where tasks have been deleted
            // but sub-tasks still exist.
            subTaskService.entrySet().stream()
                    .filter(entry -> shouldDeleteSubTasks(entry, taskIdSet))
                    .forEach(entry -> postDeleteAllSubTaskEvent(entry.getKey()));
            
            // Handle regular tasks
            tasks.forEach(this::processTask);
        } catch (Exception e) {
            log.warn("CheckRunningStatusThread Exception", e);
            metricsManager.recordError(MetricsStatisticsType.DISPATCHER_TASK);
        }
    }
    
    /**
     * Listens for the event of deleting all sub-tasks and handles the delete operation.
     *
     * @param deleteAllSubTaskEvent The event object containing the task information to be deleted.
     */
    public void listenerDeleteAllTaskEvent(DeleteAllSubTaskEvent deleteAllSubTaskEvent) {
        // Retrieve the task object
        TaskDO task = deleteAllSubTaskEvent.getTaskDO();
        
        // Retrieve the task ID
        String taskId = task.getTaskId();
        
        // Remove the set of service names corresponding to the task ID from subTaskService
        Set<String> serviceNameSet = subTaskService.remove(taskId);
        
        // If the set of service names is empty, return immediately
        if (CollectionUtils.isEmpty(serviceNameSet)) {
            return;
        }
        
        // Build the list of sub-tasks pending removal
        List<TaskDO> servicesPendingRemoval = serviceNameSet.stream()
                .map(serviceName -> buildSubTaskDO(task, serviceName))
                .collect(Collectors.toUnmodifiableList());
        
        // Handle the removal of the pending sub-tasks
        handleRemoval(servicesPendingRemoval, serviceNameSet);
    }
    
    
    private boolean shouldDeleteSubTasks(Map.Entry<String, Set<String>> entry, Set<String> taskIdSet) {
        return !taskIdSet.contains(entry.getKey()) && !entry.getValue().isEmpty();
    }
    
    private void postDeleteAllSubTaskEvent(String taskId) {
        TaskDO taskDO = new TaskDO();
        taskDO.setTaskId(taskId);
        eventBus.post(new DeleteAllSubTaskEvent(taskDO));
    }
    
    /**
     * Processes the given task by determining the services that need to be inserted and removed,
     * and performs the corresponding operations.
     *
     * @param task The task object to be processed.
     */
    private void processTask(TaskDO task) {
        // Retrieve the set of services for the task, creating a new set if it does not exist
        Set<String> serviceSet = subTaskService.computeIfAbsent(task.getTaskId(), k -> ConcurrentHashMap.newKeySet());
        
        // Get the list of all service names associated with the task
        List<String> serviceNameList = getAllServiceName(task);
        
        // Determine the services that need to be inserted (those not in the current service set)
        List<TaskDO> servicesPendingInsertion = serviceNameList.stream()
                .filter(serviceName -> !serviceSet.contains(serviceName))
                .map(serviceName -> buildSubTaskDO(task, serviceName))
                .collect(Collectors.toUnmodifiableList());
        
        // Determine the services that need to be removed (those in the current service set but not in the service name list)
        List<TaskDO> servicesPendingRemoval = serviceSet.stream()
                .filter(serviceName -> !serviceNameList.contains(serviceName))
                .map(serviceName -> buildSubTaskDO(task, serviceName))
                .collect(Collectors.toUnmodifiableList());
        
        
        // If all lists are empty, there is nothing to process
        if (CollectionUtils.isEmpty(serviceNameList) && CollectionUtils.isEmpty(servicesPendingInsertion)
                && CollectionUtils.isEmpty(servicesPendingRemoval)) {
            log.debug("No service found for task: {}", task.getTaskId());
            return;
        }
        
        
        // If the task status is SYNC, handle the insertion of services
        if (TaskStatusEnum.SYNC.getCode().equals(task.getTaskStatus())) {
            handleInsertion(servicesPendingInsertion, serviceSet);
        }
        // Handle the removal of services
        handleRemoval(servicesPendingRemoval, serviceSet);
        
        if (TaskStatusEnum.DELETE.getCode().equals(task.getTaskStatus())) {
            List<TaskDO> allSubTasks = serviceNameList.stream().map(serviceName -> buildSubTaskDO(task, serviceName))
                    .collect(Collectors.toList());
            handleRemoval(allSubTasks, serviceSet);
        }
    }
    
    /**
     * Handles the insertion of services.
     *
     * @param servicesPendingInsertion The list of services to be inserted.
     * @param serviceSet The set of services.
     */
    private void handleInsertion(List<TaskDO> servicesPendingInsertion, Set<String> serviceSet) {
        BatchTaskExecutor.batchOperation(servicesPendingInsertion, t -> {
            eventBus.post(new SyncTaskEvent(t));
            serviceSet.add(t.getServiceName());
        });
    }
    /**
     * Handles the removal of services.
     *
     * @param servicesPendingRemoval The list of services to be removed.
     * @param serviceSet The set of services.
     */
    private void handleRemoval(List<TaskDO> servicesPendingRemoval, Set<String> serviceSet) {
        BatchTaskExecutor.batchOperation(servicesPendingRemoval, t -> {
            eventBus.post(new DeleteTaskEvent(t));
            serviceSet.remove(t.getServiceName());
        });
    }
    /**
     * Builds a sub-task object for the given task and service name.
     *
     * @param serviceName The service name.
     * @return The constructed sub-task object.
     */
    private static TaskDO buildSubTaskDO(TaskDO taskDO, String serviceName) {
        TaskDO task = new TaskDO();
        
        BeanUtils.copyProperties(taskDO, task);
        task.setTaskId(SkyWalkerUtil.generateTaskId(serviceName, taskDO.getGroupName(), taskDO.getSourceClusterId(),
                taskDO.getDestClusterId()));
        task.setServiceName(serviceName);
        return task;
    }
    
    /**
     * Retrieves all service names associated with the given task.
     *
     * @return A list of service names.
     */
    private List<String> getAllServiceName(TaskDO taskDO) {
        NamingService namingService = nacosServerHolder.get(taskDO.getSourceClusterId());
        if (namingService == null) {
            log.warn("naming service is null or not found, clusterId:{}", taskDO.getSourceClusterId());
            return Collections.emptyList();
        }
        try {
            ListView<String> servicesOfServer = namingService.getServicesOfServer(0, Integer.MAX_VALUE,
                    taskDO.getGroupName());
            return servicesOfServer.getData();
        } catch (Exception e) {
            log.error("query service list failure", e);
        }
        
        return Collections.emptyList();
    }
    
    
   
}
