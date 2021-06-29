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
package com.alibaba.nacossync.event.listener;

import javax.annotation.PostConstruct;

import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.monitor.MetricsManager;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.event.SyncTaskEvent;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author NacosSync
 * @version $Id: EventListener.java, v 0.1 2018-09-27 AM1:21 NacosSync Exp $$
 */
@Slf4j
@Service
public class EventListener {

    @Autowired
    private MetricsManager metricsManager;

    @Autowired
    private SyncManagerService syncManagerService;

    @Autowired
    private EventBus eventBus;

    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;
    
    private ConcurrentHashMap<String, TaskDO> syncTaskMap = new ConcurrentHashMap<String, TaskDO>();

    private ConcurrentHashMap<String, TaskDO> deleteTaskMap = new ConcurrentHashMap<String, TaskDO>();

    @PostConstruct
    public void register() {
        eventBus.register(this);
        
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("com.alibaba.nacossync.event.retransmitter");
                return t;
            }
        });
        
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (this) {
                        ConcurrentHashMap<String, TaskDO> lastSyncTaskMap = syncTaskMap;
                        ConcurrentHashMap<String, TaskDO> lastDeleteTaskMap = deleteTaskMap;
                        syncTaskMap = new ConcurrentHashMap<String, TaskDO>();
                        deleteTaskMap = new ConcurrentHashMap<String, TaskDO>();
                    }
                    
                    for (TaskDO taskDO : lastSyncTaskMap.values()) {
                        try {
                            long start = System.currentTimeMillis();
                            if (syncManagerService.sync(taskDO)) {                
                                skyWalkerCacheServices.addFinishedTask(taskDO());
                                metricsManager.record(MetricsStatisticsType.SYNC_TASK_RT, System.currentTimeMillis() - start);
                            } else {
                                log.warn("taskId:{} sync failure", taskDO.getTaskId());
                            }
                        } catch (Exception e) {
                            log.warn("taskId:{} sync process error", taskDO.getTaskId(), e);
                        }
                    }
                    
                    for (TaskDO taskDO : lastDeleteTaskMap.values()) {
                        try {
                            long start = System.currentTimeMillis();
                            if (syncManagerService.delete(taskDO)) {
                                skyWalkerCacheServices.addFinishedTask(taskDO());
                                metricsManager.record(MetricsStatisticsType.DELETE_TASK_RT, System.currentTimeMillis() - start);
                            } else {
                                log.warn("taskId:{} delete failure", taskDO.getTaskId());
                            }
                        } catch (Exception e) {
                            log.warn("taskId:{} delete process error", taskDO.getTaskId(), e);
                        }
                    }
                } catch (Throwable e) {
                    log.warn("retransmit event error", e);
                }
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

    @Subscribe
    public void listenerSyncTaskEvent(SyncTaskEvent syncTaskEvent) {

        try {
            String taskId = syncTaskEvent.getTaskDO().getTaskId();
            synchronized (this) {
                syncTaskMap.put(taskId, syncTaskEvent.getTaskDO());
                deleteTaskMap.remove(taskId);
            }
        } catch (Exception e) {
            log.warn("listenerSyncTaskEvent process error", e);
        }

    }

    @Subscribe
    public void listenerDeleteTaskEvent(DeleteTaskEvent deleteTaskEvent) {

        try {
            String taskId = deleteTaskEvent.getTaskDO().getTaskId();
            synchronized (this) {
                deleteTaskMap.put(taskId, deleteTaskEvent.getTaskDO());
                syncTaskMap.remove(taskId);
            }
        } catch (Exception e) {
            log.warn("listenerDeleteTaskEvent process error", e);
        }

    }

}
