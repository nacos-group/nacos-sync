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

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.ConsulServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.ConsulUtils;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.model.HealthService;
import com.google.common.collect.Lists;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author zhanglong
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.CONSUL)
public class NacosSyncToConsulServiceImpl implements SyncService {
    private Map<String, EventListener> nacosListenerMap = new ConcurrentHashMap<>();

    private final MetricsManager metricsManager;

    private final SkyWalkerCacheServices skyWalkerCacheServices;

    private final NacosServerHolder nacosServerHolder;
    private final ConsulServerHolder consulServerHolder;

    public NacosSyncToConsulServiceImpl(MetricsManager metricsManager, SkyWalkerCacheServices skyWalkerCacheServices,
        NacosServerHolder nacosServerHolder, ConsulServerHolder consulServerHolder) {
        this.metricsManager = metricsManager;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.consulServerHolder = consulServerHolder;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        try {

            NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());
            ConsulClient consulClient = consulServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());

            sourceNamingService.unsubscribe(taskDO.getServiceName(), nacosListenerMap.get(taskDO.getTaskId()));

            // 删除目标集群中同步的实例列表
            Response<List<HealthService>> serviceResponse =
                consulClient.getHealthServices(taskDO.getServiceName(), true, QueryParams.DEFAULT);
            List<HealthService> healthServices = serviceResponse.getValue();
            for (HealthService healthService : healthServices) {

                if (needDelete(ConsulUtils.transferMetadata(healthService.getService().getTags()), taskDO)) {
                    consulClient.agentServiceDeregister(URLEncoder
                            .encode(healthService.getService().getId(), StandardCharsets.UTF_8.name()));
                }
            }
        } catch (Exception e) {
            log.error("delete a task from nacos to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());
            ConsulClient consulClient = consulServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());

            nacosListenerMap.putIfAbsent(taskDO.getTaskId(), event -> {
                if (event instanceof NamingEvent) {
                    try {
                        Set<String> instanceKeySet = new HashSet();
                        List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName());
                        // 先将新的注册一遍
                        for (Instance instance : sourceInstances) {
                            if (needSync(instance.getMetadata())) {
                                consulClient.agentServiceRegister(buildSyncInstance(instance, taskDO));
                                instance.getInstanceId();
                                instanceKeySet.add(composeInstanceKey(instance.getIp(), instance.getPort()));
                            }
                        }

                        // 再将不存在的删掉
                        Response<List<HealthService>> serviceResponse =
                            consulClient.getHealthServices(taskDO.getServiceName(), true, QueryParams.DEFAULT);
                        List<HealthService> healthServices = serviceResponse.getValue();
                        for (HealthService healthService : healthServices) {

                            if (needDelete(ConsulUtils.transferMetadata(healthService.getService().getTags()), taskDO)
                                && !instanceKeySet.contains(composeInstanceKey(healthService.getService().getAddress(),
                                    healthService.getService().getPort()))) {
                                consulClient.agentServiceDeregister(URLEncoder
                                    .encode(healthService.getService().getId(), StandardCharsets.UTF_8.toString()));
                            }
                        }
                    } catch (Exception e) {
                        log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                    }
                }
            });

            sourceNamingService.subscribe(taskDO.getServiceName(), nacosListenerMap.get(taskDO.getTaskId()));
        } catch (Exception e) {
            log.error("sync task from nacos to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }

    public NewService buildSyncInstance(Instance instance, TaskDO taskDO) {
        NewService newService = new NewService();
        newService.setAddress(instance.getIp());
        newService.setPort(instance.getPort());
        newService.setName(taskDO.getServiceName());
        newService.setId(instance.getInstanceId());
        List<String> tags = Lists.newArrayList();
        tags.addAll(instance.getMetadata().entrySet().stream()
            .map(entry -> String.join("=", entry.getKey(), entry.getValue())).collect(Collectors.toList()));
        tags.add(String.join("=", SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId()));
        tags.add(String.join("=", SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode()));
        tags.add(String.join("=", SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId()));
        newService.setTags(tags);
        return newService;
    }

}
