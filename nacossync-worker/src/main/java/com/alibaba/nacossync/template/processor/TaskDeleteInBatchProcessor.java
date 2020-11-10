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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDeleteInBatchRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.template.Processor;

import lombok.extern.slf4j.Slf4j;


/**
 * @author yongchao9
 * @version $Id: TaskBatchDeleteProcessor.java, v 0.3.1 2019-06-27 PM14:33 NacosSync Exp $$
 */

@Slf4j
@Service
public class TaskDeleteInBatchProcessor implements Processor<TaskDeleteInBatchRequest, BaseResult> {

    @Autowired
    private TaskAccessService taskAccessService;

    @Override
    public void process(TaskDeleteInBatchRequest taskBatchDeleteRequest, BaseResult baseResult,
                        Object... others) {
//    	
//    	String[] taskIds= taskBatchDeleteRequest.getTaskIds();
//    	List<TaskDO> taskDOs = new ArrayList<TaskDO>();
//    	for (String taskId : taskIds) {
//    		TaskDO taskDO = new TaskDO();
//    		taskDO.setTaskId(taskId);
//    		taskDOs.add(taskDO);
//		}
        taskAccessService.deleteTaskInBatch(taskBatchDeleteRequest.getTaskIds());
    }
}
