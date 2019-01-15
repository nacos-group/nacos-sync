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
package com.alibaba.nacossync.extension.event;

import com.alibaba.nacossync.pojo.model.TaskDO;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * @author paderlol
 * @date: 2019-01-12 22:42
 */
@Service
public class SpecialSyncEventBus {
    private ConcurrentHashMap<String, SpecialSyncEvent> specialSyncEventRegistry = new ConcurrentHashMap<>();

    public void subscribe(TaskDO taskDO, Consumer<TaskDO> syncAction) {
        SpecialSyncEvent specialSyncEvent = new SpecialSyncEvent();
        specialSyncEvent.setTaskDO(taskDO);
        specialSyncEvent.setSyncAction(syncAction);
        specialSyncEventRegistry.putIfAbsent(taskDO.getTaskId(), specialSyncEvent);
    }

    public void unsubscribe(TaskDO taskDO) {
        specialSyncEventRegistry.remove(taskDO.getTaskId());
    }

    public Collection<SpecialSyncEvent> getAllSpecialSyncEvent() {
        return specialSyncEventRegistry.values();
    }
}
