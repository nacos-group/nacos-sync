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

package com.alibaba.nacossync.template.processor;

import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.event.DeleteAllSubTaskEvent;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDeleteRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.template.Processor;
import com.google.common.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author NacosSync
 * @version $Id: TaskDeleteProcessor.java, v 0.1 2018-09-30 PM12:52 NacosSync Exp $$
 */
@Slf4j
@Service
public class TaskDeleteProcessor implements Processor<TaskDeleteRequest, BaseResult> {
    
    private final TaskAccessService taskAccessService;
    
    private final EventBus eventBus;
    
    public TaskDeleteProcessor(TaskAccessService taskAccessService, EventBus eventBus) {
        this.taskAccessService = taskAccessService;
        this.eventBus = eventBus;
    }
    
    @Override
    public void process(TaskDeleteRequest taskDeleteRequest, BaseResult baseResult, Object... others) {
        TaskDO taskDO = taskAccessService.findByTaskId(taskDeleteRequest.getTaskId());
        // delete all sub task when ServiceName is all
        if (SkyWalkerConstants.NACOS_ALL_SERVICE_NAME.equalsIgnoreCase(taskDO.getServiceName())) {
            eventBus.post(new DeleteAllSubTaskEvent(taskDO));
        } else {
            eventBus.post(new DeleteTaskEvent(taskDO));
        }
        log.info("删除同步任务数据之前，发出一个同步事件:{}", taskDO);
        taskAccessService.deleteTaskById(taskDeleteRequest.getTaskId());
    }
    
    
}
