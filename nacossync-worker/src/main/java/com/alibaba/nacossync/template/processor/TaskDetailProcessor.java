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

import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.result.TaskDetailQueryResult;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDetailQueryRequest;
import com.alibaba.nacossync.pojo.view.TaskModel;
import com.alibaba.nacossync.template.Processor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author NacosSync
 * @version $Id: TaskDetailProcessor.java, v 0.1 2018-09-30 PM12:52 NacosSync Exp $$
 */
@Slf4j
@Service
public class TaskDetailProcessor implements Processor<TaskDetailQueryRequest, TaskDetailQueryResult> {

    @Autowired
    private TaskAccessService taskAccessService;

    @Override
    public void process(TaskDetailQueryRequest taskDetailQueryRequest, TaskDetailQueryResult taskDetailQueryResult, Object... others)
            throws Exception {

        TaskDO taskDO = taskAccessService.findByTaskId(taskDetailQueryRequest.getTaskId());

        if (null == taskDO) {
            throw new SkyWalkerException("taskDo is null,taskId :" + taskDetailQueryRequest.getTaskId());
        }

        TaskModel taskModel = new TaskModel();

        taskModel.setDestClusterId(taskDO.getDestClusterId());
        taskModel.setGroupName(taskDO.getGroupName());
        taskModel.setServiceName(taskDO.getServiceName());
        taskModel.setSourceClusterId(taskDO.getSourceClusterId());
        taskModel.setTaskStatus(taskDO.getTaskStatus());
        taskModel.setTaskId(taskDO.getTaskId());

        taskDetailQueryResult.setTaskModel(taskModel);
    }
}
