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

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.client.naming.NacosNamingService;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.utils.HttpMethod;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskAddAllRequest;
import com.alibaba.nacossync.pojo.request.TaskAddRequest;
import com.alibaba.nacossync.pojo.result.TaskAddResult;
import com.alibaba.nacossync.template.Processor;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.ReflectionUtils;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.alibaba.nacossync.constant.SkyWalkerConstants.GROUP_NAME_PARAM;
import static com.alibaba.nacossync.constant.SkyWalkerConstants.PAGE_NO;
import static com.alibaba.nacossync.constant.SkyWalkerConstants.PAGE_SIZE;
import static com.alibaba.nacossync.constant.SkyWalkerConstants.SERVICE_NAME_PARAM;

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
        
        final EnhanceNamingService enhanceNamingService = new EnhanceNamingService(sourceNamingService);
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
    
    static class EnhanceNamingService {
        
        protected NamingService delegate;
        
        protected NamingProxy serverProxy;
        
        protected EnhanceNamingService(NamingService namingService) {
            if (!(namingService instanceof NacosNamingService)) {
                throw new IllegalArgumentException(
                        "namingService only support instance of com.alibaba.nacos.client.naming.NacosNamingService.");
            }
            this.delegate = namingService;
            
            // serverProxy
            final Field serverProxyField = ReflectionUtils.findField(NacosNamingService.class, "serverProxy");
            assert serverProxyField != null;
            ReflectionUtils.makeAccessible(serverProxyField);
            this.serverProxy = (NamingProxy) ReflectionUtils.getField(serverProxyField, delegate);
        }
        
        public CatalogServiceResult catalogServices(@Nullable String serviceName, @Nullable String group)
                throws NacosException {
            int pageNo = 1; // start with 1
            int pageSize = 100;
            
            final CatalogServiceResult result = catalogServices(serviceName, group, pageNo, pageSize);
            
            CatalogServiceResult tmpResult = result;
            
            while (Objects.nonNull(tmpResult) && tmpResult.serviceList.size() >= pageSize) {
                pageNo++;
                tmpResult = catalogServices(serviceName, group, pageNo, pageSize);
                
                if (tmpResult != null) {
                    result.serviceList.addAll(tmpResult.serviceList);
                }
            }
            
            return result;
        }
        
        /**
         * @see com.alibaba.nacos.client.naming.core.HostReactor#getServiceInfoDirectlyFromServer(String, String)
         */
        public CatalogServiceResult catalogServices(@Nullable String serviceName, @Nullable String group, int pageNo,
                int pageSize) throws NacosException {
            
            // pageNo
            // pageSize
            // serviceNameParam
            // groupNameParam
            final Map<String, String> params = new HashMap<>(8);
            params.put(CommonParams.NAMESPACE_ID, serverProxy.getNamespaceId());
            params.put(SERVICE_NAME_PARAM, serviceName);
            params.put(GROUP_NAME_PARAM, group);
            params.put(PAGE_NO, String.valueOf(pageNo));
            params.put(PAGE_SIZE, String.valueOf(pageSize));
            
            final String result = this.serverProxy.reqApi(UtilAndComs.nacosUrlBase + "/catalog/services", params,
                    HttpMethod.GET);
            if (StringUtils.isNotEmpty(result)) {
                return JacksonUtils.toObj(result, CatalogServiceResult.class);
            }
            return null;
        }
        
    }
    
    /**
     * Copy from Nacos Server.
     */
    @Data
    static class ServiceView {
        
        private String name;
        
        private String groupName;
        
        private int clusterCount;
        
        private int ipCount;
        
        private int healthyInstanceCount;
        
        private String triggerFlag;
        
    }
    
    @Data
    static class CatalogServiceResult {
        
        /**
         * count，not equal serviceList.size .
         */
        private int count;
        
        private List<ServiceView> serviceList;
        
    }
    
}
