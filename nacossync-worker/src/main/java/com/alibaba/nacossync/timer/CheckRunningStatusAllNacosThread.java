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
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.extension.TaskDispatcher;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.NacosUtils;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import com.alibaba.nacossync.util.TaskLocks;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

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
@Service
public class CheckRunningStatusAllNacosThread implements Runnable {
    
    private final Map<String, Set<String>> subTaskService = new ConcurrentHashMap<>();
    
    /**
     * The ALL tasks seen so far, so that their sub-tasks can still be built once the task row is gone.
     */
    private final Map<String, TaskDO> allTasks = new ConcurrentHashMap<>();
    
    /**
     * ALL tasks skipped because their source is not Nacos, so that each is warned about once.
     */
    private final Set<String> skippedTasks = ConcurrentHashMap.newKeySet();
    
    private final MetricsManager metricsManager;
    
    private final TaskAccessService taskAccessService;
    
    private final NacosServerHolder nacosServerHolder;
    
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    
    private final TaskDispatcher taskDispatcher;
    
    
    public CheckRunningStatusAllNacosThread(MetricsManager metricsManager, TaskAccessService taskAccessService,
            NacosServerHolder nacosServerHolder, SkyWalkerCacheServices skyWalkerCacheServices,
            TaskDispatcher taskDispatcher) {
        this.metricsManager = metricsManager;
        this.taskAccessService = taskAccessService;
        this.nacosServerHolder = nacosServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.taskDispatcher = taskDispatcher;
    }
    
    /**
     * Synchronize data based on the ns level.
     */
    @Override
    public void run() {
        
        try {
            List<TaskDO> tasks = taskAccessService.findAllByServiceNameEqualAll();
            if (tasks == null) {
                tasks = Collections.emptyList();
            }
            
            // Get the set of all task IDs. The cleanup below must also run when no ALL task is left, which is
            // exactly the case after the last one was deleted.
            Set<String> taskIdSet = tasks.stream().map(TaskDO::getTaskId).collect(Collectors.toSet());
            
            // Filter and handle sub-tasks that need to be deleted. This handles the case where tasks have been deleted
            // but sub-tasks still exist.
            subTaskService.entrySet().stream()
                    .filter(entry -> shouldDeleteSubTasks(entry, taskIdSet))
                    .forEach(entry -> deleteSubTasksOfRemovedTask(entry.getKey()));
            
            // Handle regular tasks. The rows were read before: process a task only if it is still there. A delete
            // removes the row and stops the sub-tasks under the same lock; a task whose lock is taken is left to the
            // next round.
            tasks.forEach(task -> {
                boolean ran = TaskLocks.tryRunLocked(task.getTaskId(), () -> {
                    TaskDO current = taskAccessService.findByTaskId(task.getTaskId());
                    if (current != null && hasNacosSource(current)) {
                        processTask(current);
                    }
                });
                if (!ran) {
                    log.debug("ALL task {} is locked, left to the next round", task.getTaskId());
                }
            });
        } catch (Throwable e) {
            // Throwable: the sub-tasks start on this thread, and anything escaping a run cancels the fixed-delay
            // schedule of the scan for good, without a word.
            log.warn("CheckRunningStatusThread Exception", e);
            metricsManager.recordError(MetricsStatisticsType.DISPATCHER_TASK);
        }
    }
    
    /**
     * Only a Nacos source can list its services. ALL tasks from other sources are rejected when added, but older
     * ones can still be in the database: skip them rather than treat the address of another registry as Nacos.
     */
    private boolean hasNacosSource(TaskDO task) {
        ClusterTypeEnum sourceType = skyWalkerCacheServices.getClusterType(task.getSourceClusterId());
        if (ClusterTypeEnum.NACOS == sourceType) {
            return true;
        }
        if (skippedTasks.add(task.getTaskId())) {
            log.warn("ALL task {} is skipped: its source cluster {} is {}, not Nacos. Delete the task.",
                    task.getTaskId(), task.getSourceClusterId(), sourceType);
        }
        return false;
    }
    
    /**
     * Deletes all sub-tasks of an ALL task.
     * <p>
     * One call at a time, and a failure is logged rather than thrown, as when this ran as a Guava EventBus subscriber:
     * the task row is already gone, and the delete request that called this must not fail, nor a batch delete stop.
     *
     * @param task The ALL task whose sub-tasks are deleted.
     */
    public synchronized void deleteAllSubTasks(TaskDO task) {
        try {
            // Retrieve the task ID
            String taskId = task.getTaskId();
            
            // Remove the set of service names corresponding to the task ID from subTaskService
            Set<String> serviceNameSet = subTaskService.remove(taskId);
            allTasks.remove(taskId);
            skippedTasks.remove(taskId);
            
            // If the set of service names is empty, return immediately
            if (CollectionUtils.isEmpty(serviceNameSet)) {
                return;
            }
            
            // Build the list of sub-tasks pending removal
            List<TaskDO> servicesPendingRemoval = serviceNameSet.stream()
                    .map(serviceName -> buildSubTaskDO(task, serviceName))
                    .toList();
            
            // Handle the removal of the pending sub-tasks
            handleRemoval(servicesPendingRemoval, serviceNameSet);
        } catch (Exception e) {
            log.error("Failed to delete the sub-tasks of task {}", task.getTaskId(), e);
        }
    }
    
    
    private boolean shouldDeleteSubTasks(Map.Entry<String, Set<String>> entry, Set<String> taskIdSet) {
        return !taskIdSet.contains(entry.getKey()) && !entry.getValue().isEmpty();
    }
    
    private void deleteSubTasksOfRemovedTask(String taskId) {
        // The sub-tasks are built from the task's clusters and group, so the task id alone is not enough.
        TaskDO taskDO = allTasks.remove(taskId);
        if (taskDO == null) {
            taskDO = new TaskDO();
            taskDO.setTaskId(taskId);
        }
        deleteAllSubTasks(taskDO);
    }
    
    /**
     * Processes the given task by determining the services that need to be inserted and removed,
     * and performs the corresponding operations.
     *
     * @param task The task object to be processed.
     */
    private void processTask(TaskDO task) {
        allTasks.put(task.getTaskId(), task);
        // Retrieve the set of services for the task, creating a new set if it does not exist
        Set<String> serviceSet = subTaskService.computeIfAbsent(task.getTaskId(), k -> ConcurrentHashMap.newKeySet());
        
        // Get the list of all service names associated with the task
        List<String> serviceNameList = getAllServiceName(task);
        if (serviceNameList == null) {
            // Not known this round: keep the sub-tasks as they are rather than taking every service as gone.
            return;
        }

        // Determine the services that need to be inserted (those not in the current service set)
        List<TaskDO> servicesPendingInsertion = serviceNameList.stream()
                .filter(serviceName -> !serviceSet.contains(serviceName))
                .map(serviceName -> buildSubTaskDO(task, serviceName))
                .toList();
        
        // Determine the services that need to be removed (those in the current service set but not in the service name list)
        List<TaskDO> servicesPendingRemoval = serviceSet.stream()
                .filter(serviceName -> !serviceNameList.contains(serviceName))
                .map(serviceName -> buildSubTaskDO(task, serviceName))
                .toList();
        
        
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
        // One by one on this thread, so that every sub-task has started before the task lock is released: a start
        // still running after that was unknown to a delete that came next, and outlived its parent. Running them in
        // parallel would not be faster either, as TaskDispatcher handles one sync at a time. A sub-task that did not
        // start is left out of the set, so that the next round tries again, as the timer does for any other task.
        servicesPendingInsertion.forEach(t -> {
            if (taskDispatcher.sync(t)) {
                serviceSet.add(t.getServiceName());
            }
        });
    }
    /**
     * Handles the removal of services.
     *
     * @param servicesPendingRemoval The list of services to be removed.
     * @param serviceSet The set of services.
     */
    private void handleRemoval(List<TaskDO> servicesPendingRemoval, Set<String> serviceSet) {
        // One by one on this thread, for the same reason as the insertion. A sub-task that did not stop stays in the
        // set and is tried again in the next round while its service is gone from the source.
        servicesPendingRemoval.forEach(t -> {
            if (taskDispatcher.delete(t)) {
                serviceSet.remove(t.getServiceName());
            }
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
     * @return A list of service names, or null when the source could not be asked.
     */
    private List<String> getAllServiceName(TaskDO taskDO) {
        NamingService namingService = nacosServerHolder.get(taskDO.getSourceClusterId());
        if (namingService == null) {
            log.warn("naming service is null or not found, clusterId:{}", taskDO.getSourceClusterId());
            return null;
        }
        try {
            // Nacos lists no service for an empty group; a task without a group means DEFAULT_GROUP, as elsewhere.
            ListView<String> servicesOfServer = namingService.getServicesOfServer(0, Integer.MAX_VALUE,
                    NacosUtils.getGroupNameOrDefault(taskDO.getGroupName()));
            return servicesOfServer.getData();
        } catch (Exception e) {
            log.error("query service list failure", e);
        }

        return null;
    }
    
    
   
}
