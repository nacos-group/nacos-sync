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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.pojo.FinishedTask;
import com.alibaba.nacossync.pojo.model.TaskDO;

/**
 * @author NacosSync
 * @version $Id: CleanExceedOperationIdTimer.java, v 0.1 2018-09-26 PM1:39 NacosSync Exp $$
 */
@Slf4j
@Service
public class CleanExceedOperationIdTimer implements CommandLineRunner {

    @Autowired
    private SkyWalkerCacheServices   skyWalkerCacheServices;

    @Autowired
    private TaskAccessService        taskAccessService;

    @Autowired
    private ScheduledExecutorService scheduledExecutorService;

    @Override
    public void run(String... args) {
        /** Clean up the OperationId cache once every 12 hours */
        scheduledExecutorService.scheduleWithFixedDelay(new CleanExceedOperationIdThread(), 0, 12,
            TimeUnit.HOURS);

    }

    private class CleanExceedOperationIdThread implements Runnable {

        @Override
        public void run() {

            try {

                Map<String, FinishedTask> finishedTaskMap = skyWalkerCacheServices
                    .getFinishedTaskMap();

                Iterable<TaskDO> taskDOS = taskAccessService.findAll();

                Set<String> operationIds = getDbOperations(taskDOS);

                for (String operationId : finishedTaskMap.keySet()) {

                    if (!operationIds.contains(operationId)) {

                        finishedTaskMap.remove(operationId);
                    }
                }

            } catch (Exception e) {
                log.warn("CleanExceedOperationIdThread Exception", e);
            }

        }

        private Set<String> getDbOperations(Iterable<TaskDO> taskDOS) {
            Set<String> operationIds = new HashSet<>();

            taskDOS.forEach(taskDO -> operationIds.add(taskDO.getOperationId()));
            return operationIds;
        }
    }
}
