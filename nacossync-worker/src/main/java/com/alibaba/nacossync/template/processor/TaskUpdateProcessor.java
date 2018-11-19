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

import com.alibaba.nacossync.util.SkyWalkerUtil;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskUpdateRequest;
import com.alibaba.nacossync.template.Processor;

/**
 * @author NacosSync
 * @version $Id: TaskUpdateProcessor.java, v 0.1 2018-10-17 下午11:11 NacosSync Exp $$
 */
@Slf4j
@Service
public class TaskUpdateProcessor implements Processor<TaskUpdateRequest, BaseResult> {
    @Autowired
    private TaskAccessService taskAccessService;

    @Override
    public void process(TaskUpdateRequest taskUpdateRequest, BaseResult baseResult,
                        Object... others) throws Exception {

        TaskDO taskDO = taskAccessService.findByTaskId(taskUpdateRequest.getTaskId());

        if (!TaskStatusEnum.contains(taskUpdateRequest.getTaskStatus())) {
            throw new SkyWalkerException(
                    "taskUpdateRequest.getTaskStatus() is not exist , value is :"
                            + taskUpdateRequest.getTaskStatus());
        }

        if (null == taskDO) {
            throw new SkyWalkerException("taskDo is null ,taskId is :"
                    + taskUpdateRequest.getTaskId());
        }

        taskDO.setTaskStatus(taskUpdateRequest.getTaskStatus());
        taskDO.setOperationId(SkyWalkerUtil.generateOperationId());

        taskAccessService.addTask(taskDO);
    }
}
