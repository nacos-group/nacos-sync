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
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.ConsulServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.ConsulUtils;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.model.HealthService;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author zhanglong
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.CONSUL)
public class NacosSyncToConsulServiceImpl extends AbstractNacosSync {
    
    
    private static final String DELIMITER = "=";
    
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    
    private final ConsulServerHolder consulServerHolder;
    
    public NacosSyncToConsulServiceImpl(SkyWalkerCacheServices skyWalkerCacheServices,
            ConsulServerHolder consulServerHolder) {
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.consulServerHolder = consulServerHolder;
    }
    
    
    @Override
    public String composeInstanceKey(String ip, int port) {
        return String.join(":", ip, String.valueOf(port));
    }
    
    @Override
    public void register(TaskDO taskDO, Instance instance) {
        ConsulClient consulClient = consulServerHolder.get(taskDO.getDestClusterId());
        consulClient.agentServiceRegister(buildSyncInstance(instance, taskDO));
    }
    
    @Override
    public void deregisterInstance(TaskDO taskDO) throws Exception {
        ConsulClient consulClient = consulServerHolder.get(taskDO.getDestClusterId());
        Response<List<HealthService>> serviceResponse = consulClient.getHealthServices(taskDO.getServiceName(), true,
                QueryParams.DEFAULT);
        List<HealthService> healthServices = serviceResponse.getValue();
        for (HealthService healthService : healthServices) {
            
            if (needDelete(ConsulUtils.transferMetadata(healthService.getService().getTags()), taskDO)) {
                consulClient.agentServiceDeregister(
                        URLEncoder.encode(healthService.getService().getId(), StandardCharsets.UTF_8));
            }
        }
    }
    
    @Override
    public void removeInvalidInstance(TaskDO taskDO, Set<String> invalidInstanceKeys)
            throws UnsupportedEncodingException {
        ConsulClient consulClient = consulServerHolder.get(taskDO.getDestClusterId());
        Response<List<HealthService>> serviceResponse = consulClient.getHealthServices(taskDO.getServiceName(), true,
                QueryParams.DEFAULT);
        List<HealthService> healthServices = serviceResponse.getValue();
        for (HealthService healthService : healthServices) {
            
            if (needDelete(ConsulUtils.transferMetadata(healthService.getService().getTags()), taskDO)
                    && !invalidInstanceKeys.contains(composeInstanceKey(healthService.getService().getAddress(),
                    healthService.getService().getPort()))) {
                consulClient.agentServiceDeregister(
                        URLEncoder.encode(healthService.getService().getId(), StandardCharsets.UTF_8));
            }
        }
    }
    
    public NewService buildSyncInstance(Instance instance, TaskDO taskDO) {
        NewService newService = new NewService();
        newService.setAddress(instance.getIp());
        newService.setPort(instance.getPort());
        newService.setName(taskDO.getServiceName());
        newService.setId(instance.getInstanceId());
        List<String> tags = Lists.newArrayList();
        tags.addAll(instance.getMetadata().entrySet().stream()
                .map(entry -> String.join(DELIMITER, entry.getKey(), entry.getValue())).collect(Collectors.toList()));
        tags.add(String.join(DELIMITER, SkyWalkerConstants.DEST_CLUSTER_ID_KEY, taskDO.getDestClusterId()));
        tags.add(String.join(DELIMITER, SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode()));
        tags.add(String.join(DELIMITER, SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, taskDO.getSourceClusterId()));
        newService.setTags(tags);
        return newService;
    }
    
}
