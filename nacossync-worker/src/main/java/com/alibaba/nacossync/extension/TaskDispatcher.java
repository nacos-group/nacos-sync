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

package com.alibaba.nacossync.extension;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


/**
 * Starts and stops the sync of a task, in the caller's thread.
 */
@Slf4j
@Service
public class TaskDispatcher {
    
    /**
     * One sync and, apart from it, one delete at a time, as when both ran as Guava EventBus subscribers: a subscriber
     * not marked thread-safe is called by one thread at a time. The timers and the delete requests call these from
     * different threads, and the sync services have never run concurrently. Callers may hold a TaskLocks lock;
     * nothing in here takes one.
     */
    private final Object syncLock = new Object();
    
    private final Object deleteLock = new Object();
    
    private final MetricsManager metricsManager;
    
    private final SyncManagerService syncManagerService;
    
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    
    public TaskDispatcher(MetricsManager metricsManager, SyncManagerService syncManagerService,
            SkyWalkerCacheServices skyWalkerCacheServices) {
        this.metricsManager = metricsManager;
        this.syncManagerService = syncManagerService;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
    }
    
    /**
     * @return whether the task was started; a failure is logged, not thrown
     */
    public boolean sync(TaskDO taskDO) {
        synchronized (syncLock) {

            try {

                Stopwatch stopwatch = Stopwatch.createStarted();
                if (syncManagerService.sync(taskDO, null)) {
                    skyWalkerCacheServices.addFinishedTask(taskDO);
                    metricsManager.record(MetricsStatisticsType.SYNC_TASK_RT, stopwatch.elapsed().toMillis());
                    return true;
                }
                log.warn("syncTaskEvent process error");
            } catch (Exception e) {
                log.warn("syncTaskEvent process error", e);
            }
            return false;
        }
    }

    /**
     * @return whether the task was stopped; a failure is logged, not thrown
     */
    public boolean delete(TaskDO taskDO) {
        synchronized (deleteLock) {

            try {
                Stopwatch stopwatch = Stopwatch.createStarted();
                if (syncManagerService.delete(taskDO)) {
                    skyWalkerCacheServices.removeFinishedTask(taskDO.getOperationId());
                    metricsManager.record(MetricsStatisticsType.DELETE_TASK_RT, stopwatch.elapsed().toMillis());
                    return true;
                }
                log.warn("deleteTaskEvent delete failure");
            } catch (Exception e) {
                log.warn("deleteTaskEvent delete failure.", e);
            }
            return false;
        }
    }
}
