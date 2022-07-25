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

package com.alibaba.nacossync.timer;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 *  when the database task service name is empty, check all the services in the cluster and create a synchronization task.
 * @ClassName: CheckRunningStatusAllThread
 * @Author: ChenHao26
 * @Date: 2022/7/20 10:30
 * @Description: muti sync data
 */
@Slf4j
public class CheckRunningStatusAllThread implements Runnable{
    
    private MetricsManager metricsManager;
    
    private SkyWalkerCacheServices skyWalkerCacheServices;
    
    private TaskAccessService taskAccessService;
    
    private EventBus eventBus;
    
    private NacosServerHolder nacosServerHolder;
    
    private FastSyncHelper fastSyncHelper;
    
    public CheckRunningStatusAllThread(MetricsManager metricsManager, SkyWalkerCacheServices skyWalkerCacheServices,
            TaskAccessService taskAccessService, EventBus eventBus, NacosServerHolder nacosServerHolder,
            FastSyncHelper fastSyncHelper) {
        this.metricsManager = metricsManager;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.taskAccessService = taskAccessService;
        this.eventBus = eventBus;
        this.nacosServerHolder = nacosServerHolder;
        this.fastSyncHelper = fastSyncHelper;
    }
    
    /**
     * 根据ns级别进行数据同步
     */
    @Override
    public void run() {
        Long startTime = System.currentTimeMillis();
        try {
            List<TaskDO> taskDOS = taskAccessService.findServiceNameIsNull()
                    .stream().filter(t -> t.getStatus() == null || t.getStatus() == 0)
                    .collect(Collectors.toList());
    
            for (TaskDO taskDO : taskDOS) {
                List<String> serviceNameList = getServiceNameList(taskDO);
                if (CollectionUtils.isEmpty(serviceNameList)) {
                   continue;
                }
                
                //如果是null，证明此时没有处理完成
                List<String> filterService = serviceNameList.stream()
                        .filter(s -> skyWalkerCacheServices.getFinishedTask(taskDO.getTaskId() + s ) == null)
                        .collect(Collectors.toList());
                
                if (CollectionUtils.isEmpty(filterService)) {
                    continue;
                }
                
                // 当删除任务后，此时任务的状态为DELETE,不会执行数据同步
                if (TaskStatusEnum.SYNC.getCode().equals(taskDO.getTaskStatus())) {
                    fastSyncHelper.syncWithThread(taskDO, filterService);
                }
            }
        }catch (Exception e) {
            log.warn("CheckRunningStatusThread Exception ", e);
        }
        metricsManager.record(MetricsStatisticsType.DISPATCHER_TASK, System.currentTimeMillis() - startTime);
    }
    
    /**
     * get serviceName list.
     * @param taskDO task info
     * @return service list or empty list
     */
    private List<String> getServiceNameList(TaskDO taskDO) {
        NamingService namingService = nacosServerHolder.get(taskDO.getSourceClusterId());
        try {
            ListView<String> servicesOfServer = namingService.getServicesOfServer(0, Integer.MAX_VALUE,
                    taskDO.getGroupName());
            return servicesOfServer.getData();
        } catch (Exception e) {
            log.error("query service list failure",e);
        }
        
        return Collections.emptyList();
    }
}
