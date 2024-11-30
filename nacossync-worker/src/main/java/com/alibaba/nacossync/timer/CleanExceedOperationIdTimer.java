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
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.pojo.FinishedTask;
import com.alibaba.nacossync.pojo.model.TaskDO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author NacosSync
 * @version $Id: CleanExceedOperationIdTimer.java, v 0.1 2018-09-26 PM1:39 NacosSync Exp $$
 */
@Slf4j
@Service
public class CleanExceedOperationIdTimer implements CommandLineRunner {
    
    private static final long INITIAL_DELAY = 0;
    
    private static final long PERIOD = 12;
    
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    
    private final TaskAccessService taskAccessService;
    
    private final ScheduledExecutorService scheduledExecutorService;
    
    public CleanExceedOperationIdTimer(SkyWalkerCacheServices skyWalkerCacheServices,
            TaskAccessService taskAccessService, ScheduledExecutorService scheduledExecutorService) {
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.taskAccessService = taskAccessService;
        this.scheduledExecutorService = scheduledExecutorService;
    }
    
    @Override
    public void run(String... args) {
        /** Clean up the OperationId cache once every 12 hours */
        scheduledExecutorService.scheduleWithFixedDelay(new CleanExceedOperationIdThread(), INITIAL_DELAY, PERIOD,
                TimeUnit.HOURS);
        log.info("CleanExceedOperationIdTimer has started successfully");
        
    }
    
    private class CleanExceedOperationIdThread implements Runnable {
        
        @Override
        public void run() {
            
            try {
                
                Map<String, FinishedTask> finishedTaskMap = skyWalkerCacheServices.getFinishedTaskMap();
                Set<String> operationIds = getDbOperations(taskAccessService.findAll());
                finishedTaskMap.keySet().removeIf(operationId -> !operationIds.contains(operationId));
                
            } catch (Exception e) {
                log.warn("CleanExceedOperationIdThread Exception", e);
            }
            
        }
        
        private Set<String> getDbOperations(Iterable<TaskDO> taskDOS) {
            return StreamSupport.stream(taskDOS.spliterator(), false).map(TaskDO::getOperationId)
                    .collect(Collectors.toSet());
        }
    }
}
