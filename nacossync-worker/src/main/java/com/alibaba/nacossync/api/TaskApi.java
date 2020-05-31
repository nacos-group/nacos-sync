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
package com.alibaba.nacossync.api;

import com.alibaba.nacossync.pojo.request.TaskAddRequest;
import com.alibaba.nacossync.pojo.request.TaskDeleteInBatchRequest;
import com.alibaba.nacossync.pojo.request.TaskDeleteRequest;
import com.alibaba.nacossync.pojo.request.TaskDetailQueryRequest;
import com.alibaba.nacossync.pojo.request.TaskListQueryRequest;
import com.alibaba.nacossync.pojo.request.TaskUpdateRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.pojo.result.TaskAddResult;
import com.alibaba.nacossync.pojo.result.TaskDetailQueryResult;
import com.alibaba.nacossync.pojo.result.TaskListQueryResult;
import com.alibaba.nacossync.template.SkyWalkerTemplate;
import com.alibaba.nacossync.template.processor.TaskAddProcessor;
import com.alibaba.nacossync.template.processor.TaskDeleteInBatchProcessor;
import com.alibaba.nacossync.template.processor.TaskDeleteProcessor;
import com.alibaba.nacossync.template.processor.TaskDetailProcessor;
import com.alibaba.nacossync.template.processor.TaskListQueryProcessor;
import com.alibaba.nacossync.template.processor.TaskUpdateProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author NacosSync
 * @version $Id: Task.java, v 0.1 2018-09-24 PM3:43 NacosSync Exp $$
 */
@Slf4j
@RestController
public class TaskApi {

    private final TaskUpdateProcessor taskUpdateProcessor;

    private final TaskAddProcessor taskAddProcessor;

    private final TaskDeleteProcessor taskDeleteProcessor;

    private final TaskDeleteInBatchProcessor taskDeleteInBatchProcessor;

    private final TaskListQueryProcessor taskListQueryProcessor;

    private final TaskDetailProcessor taskDetailProcessor;

    public TaskApi(TaskUpdateProcessor taskUpdateProcessor, TaskAddProcessor taskAddProcessor,
        TaskDeleteProcessor taskDeleteProcessor, TaskDeleteInBatchProcessor taskDeleteInBatchProcessor,
        TaskListQueryProcessor taskListQueryProcessor, TaskDetailProcessor taskDetailProcessor) {
        this.taskUpdateProcessor = taskUpdateProcessor;
        this.taskAddProcessor = taskAddProcessor;
        this.taskDeleteProcessor = taskDeleteProcessor;
        this.taskDeleteInBatchProcessor = taskDeleteInBatchProcessor;
        this.taskListQueryProcessor = taskListQueryProcessor;
        this.taskDetailProcessor = taskDetailProcessor;
    }

    @RequestMapping(path = "/v1/task/list", method = RequestMethod.GET)
    public TaskListQueryResult tasks(TaskListQueryRequest taskListQueryRequest) {

        return SkyWalkerTemplate.run(taskListQueryProcessor, taskListQueryRequest, new TaskListQueryResult());
    }

    @RequestMapping(path = "/v1/task/detail", method = RequestMethod.GET)
    public TaskDetailQueryResult getByTaskId(TaskDetailQueryRequest taskDetailQueryRequest) {

        return SkyWalkerTemplate.run(taskDetailProcessor, taskDetailQueryRequest, new TaskDetailQueryResult());
    }

    @RequestMapping(path = "/v1/task/delete", method = RequestMethod.DELETE)
    public BaseResult deleteTask(TaskDeleteRequest taskDeleteRequest) {

        return SkyWalkerTemplate.run(taskDeleteProcessor, taskDeleteRequest, new BaseResult());
    }

    /**
     * @author yongchao9
     * @param taskBatchDeleteRequest
     * @return
     */
    @RequestMapping(path = "/v1/task/deleteInBatch", method = RequestMethod.DELETE)
    public BaseResult batchDeleteTask(TaskDeleteInBatchRequest taskBatchDeleteRequest) {
        return SkyWalkerTemplate.run(taskDeleteInBatchProcessor, taskBatchDeleteRequest, new BaseResult());
    }

    @RequestMapping(path = "/v1/task/add", method = RequestMethod.POST)
    public BaseResult taskAdd(@RequestBody TaskAddRequest addTaskRequest) {

        return SkyWalkerTemplate.run(taskAddProcessor, addTaskRequest, new TaskAddResult());
    }

    @RequestMapping(path = "/v1/task/update", method = RequestMethod.POST)
    public BaseResult updateTask(@RequestBody TaskUpdateRequest taskUpdateRequest) {

        return SkyWalkerTemplate.run(taskUpdateProcessor, taskUpdateRequest, new BaseResult());
    }
}
