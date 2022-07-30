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

package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.LeaseInfo;
import com.netflix.appinfo.MyDataCenterInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhanglong
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.EUREKA)
public class NacosSyncToEurekaServiceImpl extends AbstractNacosSync {
    
    
    private final EurekaServerHolder eurekaServerHolder;
    
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    
    public NacosSyncToEurekaServiceImpl(EurekaServerHolder eurekaServerHolder,
            SkyWalkerCacheServices skyWalkerCacheServices) {
        this.eurekaServerHolder = eurekaServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
    }
    
    
    @Override
    public String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }
    
    @Override
    public void register(TaskDO taskDO, Instance instance) {
        EurekaNamingService destNamingService = eurekaServerHolder.get(taskDO.getDestClusterId());
        destNamingService.registerInstance(buildSyncInstance(instance, taskDO));
    }
    
    @Override
    public void deregisterInstance(TaskDO taskDO) {
        EurekaNamingService destNamingService = eurekaServerHolder.get(taskDO.getDestClusterId());
        List<InstanceInfo> allInstances = destNamingService.getApplications(taskDO.getServiceName());
        if (allInstances != null) {
            for (InstanceInfo instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO)) {
                    destNamingService.deregisterInstance(instance);
                }
            }
        }
    }
    
    @Override
    public void removeInvalidInstance(TaskDO taskDO, Set<String> invalidInstanceKeys) {
        EurekaNamingService destNamingService = eurekaServerHolder.get(taskDO.getDestClusterId());
        List<InstanceInfo> allInstances = destNamingService.getApplications(taskDO.getServiceName());
        if (CollectionUtils.isNotEmpty(allInstances)) {
            for (InstanceInfo instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO) && invalidInstanceKeys.contains(
                        composeInstanceKey(instance.getIPAddr(), instance.getPort()))) {
                    destNamingService.deregisterInstance(instance);
                }
            }
        }
    }
    
    
    private InstanceInfo buildSyncInstance(Instance instance, TaskDO taskDO) {
        DataCenterInfo dataCenterInfo = new MyDataCenterInfo(DataCenterInfo.Name.MyOwn);
        final Map<String, String> instanceMetadata = instance.getMetadata();
        HashMap<String, String> metadata = new HashMap<>(16);
        metadata.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metadata.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        metadata.putAll(instanceMetadata);
        String homePageUrl = obtainHomePageUrl(instance, instanceMetadata);
        String serviceName = taskDO.getServiceName();
        
        return new InstanceInfo(instance.getIp() + ":" + serviceName + ":" + instance.getPort(), serviceName, null,
                instance.getIp(), null, new InstanceInfo.PortWrapper(true, instance.getPort()), null,
                homePageUrl + "/actuator/env", homePageUrl + "/actuator/info", homePageUrl + "/actuator/health", null,
                serviceName, serviceName, 1, dataCenterInfo, instance.getIp(), InstanceInfo.InstanceStatus.UP,
                InstanceInfo.InstanceStatus.UNKNOWN, null, new LeaseInfo(30, 90, 0L, 0L, 0L, 0L, 0L), false, metadata,
                System.currentTimeMillis(), System.currentTimeMillis(), null, null);
    }
    
    private String obtainHomePageUrl(Instance instance, Map<String, String> instanceMetadata) {
        final String managementContextPath = obtainManagementContextPath(instanceMetadata);
        final String managementPort = instanceMetadata.getOrDefault(SkyWalkerConstants.MANAGEMENT_PORT_KEY,
                String.valueOf(instance.getPort()));
        return String.format("http://%s:%s%s", instance.getIp(), managementPort, managementContextPath);
    }
    
    private String obtainManagementContextPath(Map<String, String> instanceMetadata) {
        final String path = instanceMetadata.getOrDefault(SkyWalkerConstants.MANAGEMENT_CONTEXT_PATH_KEY, "");
        if (path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }
    
    
}
