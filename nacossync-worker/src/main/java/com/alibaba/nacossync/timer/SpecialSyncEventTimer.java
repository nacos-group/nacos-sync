/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.alibaba.nacossync.timer;

import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.extension.event.SpecialSyncEvent;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.google.common.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author paderlol
 * @date: 2019-01-12 22:53
 */
@Slf4j
@Service
public class SpecialSyncEventTimer implements CommandLineRunner {

    private final SpecialSyncEventBus specialSyncEventBus;

    private final EventBus eventBus;

    private final ScheduledExecutorService scheduledExecutorService;
    
    public SpecialSyncEventTimer(SpecialSyncEventBus specialSyncEventBus, EventBus eventBus,
            ScheduledExecutorService scheduledExecutorService) {
        this.specialSyncEventBus = specialSyncEventBus;
        this.eventBus = eventBus;
        this.scheduledExecutorService = scheduledExecutorService;
    }
    
    @Override
    public void run(String... args) throws Exception {

        scheduledExecutorService.scheduleWithFixedDelay(new SpecialSyncEventTimer.SpecialSyncEventThread(), 0, 3000,
                TimeUnit.MILLISECONDS);
        log.info("SpecialSyncEventTimer has started successfully");
    }

    private class SpecialSyncEventThread implements Runnable {

        @Override
        public void run() {
            try {
                Collection<SpecialSyncEvent> allSpecialSyncEvent = specialSyncEventBus.getAllSpecialSyncEvent();
                allSpecialSyncEvent.stream()
                        .filter(specialSyncEvent -> TaskStatusEnum.SYNC.getCode()
                                .equals(specialSyncEvent.getTaskDO().getTaskStatus()))
                        .forEach(eventBus::post);
            } catch (Exception e) {
               log.error("Exception occurred while processing special sync events", e);

            }
        }
    }
}
