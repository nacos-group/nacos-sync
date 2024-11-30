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

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.event.SyncTaskEvent;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author NacosSync
 * @version $Id: SkyWalkerServices.java, v 0.1 2018-09-26 AM1:39 NacosSync Exp $$
 */
@Slf4j
@Service
public class QuerySyncTaskTimer implements CommandLineRunner {
    
    private static final int INITIAL_DELAY = 0;
    
    private static final int DELAY = 3000;
    
    private final MetricsManager metricsManager;

    private final SkyWalkerCacheServices skyWalkerCacheServices;

    private final TaskAccessService taskAccessService;

    private final EventBus eventBus;

    private final ScheduledExecutorService scheduledExecutorService;
    
    private final NacosServerHolder nacosServerHolder;
    
    public QuerySyncTaskTimer(MetricsManager metricsManager, SkyWalkerCacheServices skyWalkerCacheServices,
            TaskAccessService taskAccessService, EventBus eventBus, ScheduledExecutorService scheduledExecutorService,
            NacosServerHolder nacosServerHolder) {
        this.metricsManager = metricsManager;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.taskAccessService = taskAccessService;
        this.eventBus = eventBus;
        this.scheduledExecutorService = scheduledExecutorService;
        this.nacosServerHolder = nacosServerHolder;
    }
    
    @Override
    public void run(String... args) {
        /** Fetch the task list from the database every 3 seconds */
        scheduledExecutorService.scheduleWithFixedDelay(new CheckRunningStatusThread(), INITIAL_DELAY, DELAY,
                TimeUnit.MILLISECONDS);
        
        scheduledExecutorService.scheduleWithFixedDelay(new CheckRunningStatusAllNacosThread(metricsManager,
                taskAccessService, nacosServerHolder, eventBus), INITIAL_DELAY, DELAY,
                TimeUnit.MILLISECONDS);
        log.info("QuerySyncTaskTimer has started successfully");
    }

    private class CheckRunningStatusThread implements Runnable {

        @Override
        public void run() {

            long start = System.currentTimeMillis();
            try {

                List<TaskDO> taskDOS = taskAccessService.findAllByServiceNameNotEqualAll();

                taskDOS.forEach(taskDO -> {

                    if ((null != skyWalkerCacheServices.getFinishedTask(taskDO))) {

                        return;
                    }

                    if (TaskStatusEnum.SYNC.getCode().equals(taskDO.getTaskStatus())) {

                        eventBus.post(new SyncTaskEvent(taskDO));
                        log.info("从数据库中查询到一个同步任务，发出一个同步事件:{}", taskDO);
                    }

                    if (TaskStatusEnum.DELETE.getCode().equals(taskDO.getTaskStatus())) {

                        eventBus.post(new DeleteTaskEvent(taskDO));
                        log.info("从数据库中查询到一个删除任务，发出一个同步事件:{}", taskDO);
                    }
                });

            } catch (Exception e) {
                log.warn("CheckRunningStatusThread Exception", e);
            }

            metricsManager.record(MetricsStatisticsType.DISPATCHER_TASK, System.currentTimeMillis() - start);
        }
    }
}
