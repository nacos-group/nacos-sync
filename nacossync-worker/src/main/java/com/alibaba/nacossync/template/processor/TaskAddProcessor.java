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

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.result.TaskAddResult;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskAddRequest;
import com.alibaba.nacossync.template.Processor;
import com.alibaba.nacossync.util.SkyWalkerUtil;

/**
 * @author NacosSync
 * @version $Id: TaskAddProcessor.java, v 0.1 2018-09-30 上午11:40 NacosSync Exp $$
 */
@Slf4j
@Service
public class TaskAddProcessor implements Processor<TaskAddRequest, TaskAddResult> {

    @Autowired
    private TaskAccessService    taskAccessService;

    @Autowired
    private ClusterAccessService clusterAccessService;

    @Override
    public void process(TaskAddRequest taskAddRequest, TaskAddResult taskAddResult,
                        Object... others) throws Exception {

        ClusterDO destCluster = clusterAccessService.findByClusterId(taskAddRequest
            .getDestClusterId());

        ClusterDO sourceCluster = clusterAccessService.findByClusterId(taskAddRequest
            .getSourceClusterId());

        if (null == destCluster || null == sourceCluster) {

            throw new SkyWalkerException("请检查源或者目标集群是否存在");

        }

        if (!ClusterTypeEnum.CS.getCode().equals(sourceCluster.getClusterType())
            || !ClusterTypeEnum.NACOS.getCode().equals(destCluster.getClusterType())) {

            throw new SkyWalkerException("请检查是否支持源到目标集群类型的同步！目前只支持CS->NACOS");

        }

        String taskId = SkyWalkerUtil.generateTaskId(taskAddRequest);

        TaskDO taskDO = taskAccessService.findByTaskId(taskId);

        if (null == taskDO) {

            taskDO = new TaskDO();
            taskDO.setTaskId(taskId);
            taskDO.setDestClusterId(taskAddRequest.getDestClusterId());
            taskDO.setSourceClusterId(taskAddRequest.getSourceClusterId());
            taskDO.setServiceName(taskAddRequest.getServiceName());
            taskDO.setGroupName(taskAddRequest.getGroupName());
            taskDO.setTaskStatus(TaskStatusEnum.SYNC.getCode());
            taskDO.setWorkerIp(SkyWalkerUtil.getLocalIp());
            taskDO.setOperationId(SkyWalkerUtil.generateOperationId());

        } else {

            taskDO.setTaskStatus(TaskStatusEnum.SYNC.getCode());
            taskDO.setOperationId(SkyWalkerUtil.generateOperationId());
        }

        taskAccessService.addTask(taskDO);
    }
}
