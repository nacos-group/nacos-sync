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
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.event.SyncTaskEvent;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author NacosSync
 * @version $Id: SkyWalkerServices.java, v 0.1 2018-09-26 上午1:39 NacosSync Exp $$
 */
@Slf4j
@Service
public class QuerySyncTaskTimer implements CommandLineRunner {

    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;

    @Autowired
    private TaskAccessService taskAccessService;

    @Autowired
    private EventBus eventBus;

    @Autowired
    private ScheduledExecutorService scheduledExecutorService;

    @Override
    public void run(String... args) {
        /** 3s去数据库捞一次任务列表 */
        scheduledExecutorService.scheduleWithFixedDelay(new CheckRunningStatusThread(), 0, 3000,
                TimeUnit.MILLISECONDS);

    }

    private class CheckRunningStatusThread implements Runnable {

        @Override
        public void run() {

            try {

                Iterable<TaskDO> taskDOS = taskAccessService.findAll();

                taskDOS.forEach(taskDO -> {

                    if ((null != skyWalkerCacheServices.getFinishedTask(taskDO))) {

                        return;
                    }

                    if (TaskStatusEnum.SYNC.getCode().equals(taskDO.getTaskStatus())) {

                        eventBus.post(new SyncTaskEvent(taskDO));
                        log.info("从数据库中查询到一个同步任务，发出一个同步事件:" + taskDO);
                    }

                    if (TaskStatusEnum.DELETE.getCode().equals(taskDO.getTaskStatus())) {

                        eventBus.post(new DeleteTaskEvent(taskDO));
                        log.info("从数据库中查询到一个删除任务，发出一个同步事件:" + taskDO);
                    }
                });

            } catch (Exception e) {
                log.warn("CheckRunningStatusThread Exception", e);
            }

        }
    }
}
