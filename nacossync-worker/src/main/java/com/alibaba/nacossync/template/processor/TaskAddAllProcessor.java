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

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.CatalogServiceResult;
import com.alibaba.nacossync.pojo.request.TaskAddAllRequest;
import com.alibaba.nacossync.pojo.request.TaskAddRequest;
import com.alibaba.nacossync.pojo.result.TaskAddResult;
import com.alibaba.nacossync.pojo.view.ServiceView;
import com.alibaba.nacossync.service.NacosEnhanceNamingService;
import com.alibaba.nacossync.template.Processor;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 * @author NacosSync
 * @version $Id: TaskAddAllProcessor.java, v 0.1 2022-03-23 PM11:40 NacosSync Exp $$
 */
@Slf4j
@Service
public class TaskAddAllProcessor implements Processor<TaskAddAllRequest, TaskAddResult> {
    
    private static final String CONSUMER_PREFIX = "consumers:";
    
    private final NacosServerHolder nacosServerHolder;
    
    private final SyncManagerService syncManagerService;
    
    private final TaskAccessService taskAccessService;
    
    private final ClusterAccessService clusterAccessService;
    
    public TaskAddAllProcessor(NacosServerHolder nacosServerHolder, SyncManagerService syncManagerService,
            TaskAccessService taskAccessService, ClusterAccessService clusterAccessService) {
        this.nacosServerHolder = nacosServerHolder;
        this.syncManagerService = syncManagerService;
        this.taskAccessService = taskAccessService;
        this.clusterAccessService = clusterAccessService;
    }
    
    @Override
    public void process(TaskAddAllRequest addAllRequest, TaskAddResult taskAddResult, Object... others)
            throws Exception {
        
        ClusterDO destCluster = clusterAccessService.findByClusterId(addAllRequest.getDestClusterId());
        
        ClusterDO sourceCluster = clusterAccessService.findByClusterId(addAllRequest.getSourceClusterId());
        
        if (Objects.isNull(destCluster) || Objects.isNull(sourceCluster)) {
            throw new SkyWalkerException("Please check if the source or target cluster exists.");
        }
        
        if (Objects.isNull(syncManagerService.getSyncService(sourceCluster.getClusterId(), destCluster.getClusterId()))) {
            throw new SkyWalkerException("current sync type not supported.");
        }
        // TODO 目前仅支持 Nacos 为源的同步类型，待完善更多类型支持。
        final NamingService sourceNamingService = nacosServerHolder.get(sourceCluster.getClusterId());
        if (sourceNamingService == null) {
            throw new SkyWalkerException("only support sync type that the source of the Nacos.");
        }
        
        final NacosEnhanceNamingService enhanceNamingService = new NacosEnhanceNamingService(sourceNamingService);
        final CatalogServiceResult catalogServiceResult = enhanceNamingService.catalogServices(null, null);
        if (catalogServiceResult == null || catalogServiceResult.getCount() <= 0) {
            throw new SkyWalkerException("sourceCluster data empty");
        }
        
        for (ServiceView serviceView : catalogServiceResult.getServiceList()) {
            // exclude subscriber
            if (addAllRequest.isExcludeConsumer() && serviceView.getName().startsWith(CONSUMER_PREFIX)) {
                continue;
            }
            TaskAddRequest taskAddRequest = new TaskAddRequest();
            taskAddRequest.setSourceClusterId(sourceCluster.getClusterId());
            taskAddRequest.setDestClusterId(destCluster.getClusterId());
            taskAddRequest.setServiceName(serviceView.getName());
            taskAddRequest.setGroupName(serviceView.getGroupName());
            this.dealTask(addAllRequest, taskAddRequest);
        }
    }
    
    private void dealTask(TaskAddAllRequest addAllRequest, TaskAddRequest taskAddRequest) throws Exception {
        
        String taskId = SkyWalkerUtil.generateTaskId(taskAddRequest);
        TaskDO taskDO = taskAccessService.findByTaskId(taskId);
        if (null == taskDO) {
            taskDO = new TaskDO();
            taskDO.setTaskId(taskId);
            taskDO.setDestClusterId(addAllRequest.getDestClusterId());
            taskDO.setSourceClusterId(addAllRequest.getSourceClusterId());
            taskDO.setServiceName(taskAddRequest.getServiceName());
            taskDO.setVersion(taskAddRequest.getVersion());
            taskDO.setGroupName(taskAddRequest.getGroupName());
            taskDO.setNameSpace(taskAddRequest.getNameSpace());
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
