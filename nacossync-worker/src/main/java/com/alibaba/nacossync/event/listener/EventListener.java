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

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.event.SyncTaskEvent;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

/**
 * @author NacosSync
 * @version $Id: EventListener.java, v 0.1 2018-09-27 上午1:21 NacosSync Exp $$
 */
@Slf4j
@Service
public class EventListener {

    @Autowired
    private SyncManagerService     syncManagerService;

    @Autowired
    private EventBus               eventBus;

    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;

    @PostConstruct
    public void register() {
        eventBus.register(this);
    }

    @Subscribe
    public void listenerSyncTaskEvent(SyncTaskEvent syncTaskEvent) {

        try {

            syncManagerService.sync(syncTaskEvent.getTaskDO());
            skyWalkerCacheServices.addFinishedTask(syncTaskEvent.getTaskDO());

        } catch (Exception e) {
            log.warn("listenerSyncTaskEvent process error", e);
        }

    }

    @Subscribe
    public void listenerDeleteTaskEvent(DeleteTaskEvent deleteTaskEvent) {

        try {

            syncManagerService.delete(deleteTaskEvent.getTaskDO());
            skyWalkerCacheServices.addFinishedTask(deleteTaskEvent.getTaskDO());

        } catch (Exception e) {
            log.warn("listenerDeleteTaskEvent process error", e);
        }

    }

}
