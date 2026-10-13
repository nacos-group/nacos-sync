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
import com.alibaba.nacossync.pojo.request.TaskDeleteInBatchRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.template.Processor;
import com.google.common.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;


/**
 * @author yongchao9
 * @version $Id: TaskBatchDeleteProcessor.java, v 0.3.1 2019-06-27 PM14:33 NacosSync Exp $$
 */

@Slf4j
@Service
public class TaskDeleteInBatchProcessor implements Processor<TaskDeleteInBatchRequest, BaseResult> {

    private final TaskAccessService taskAccessService;
    
    private final EventBus eventBus;
    
    public TaskDeleteInBatchProcessor(TaskAccessService taskAccessService, EventBus eventBus) {
        this.taskAccessService = taskAccessService;
        this.eventBus = eventBus;
    }
    
    @Override
    public void process(TaskDeleteInBatchRequest taskBatchDeleteRequest, BaseResult baseResult,
                        Object... others) {
        // Stop each task the way a single delete does: removing only the rows left the syncs running and
        // their instances synced in the destination.
        for (String taskId : taskBatchDeleteRequest.getTaskIds()) {
            TaskDO taskDO = taskAccessService.findByTaskId(taskId);
            if (taskDO == null) {
                continue;
            }
            if (SkyWalkerConstants.NACOS_ALL_SERVICE_NAME.equalsIgnoreCase(taskDO.getServiceName())) {
                eventBus.post(new DeleteAllSubTaskEvent(taskDO));
            } else {
                eventBus.post(new DeleteTaskEvent(taskDO));
            }
        }
        taskAccessService.deleteTaskInBatch(taskBatchDeleteRequest.getTaskIds());
    }
}
