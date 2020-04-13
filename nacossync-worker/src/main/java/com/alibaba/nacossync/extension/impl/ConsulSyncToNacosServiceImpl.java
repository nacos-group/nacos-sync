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
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.ConsulServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.ConsulUtils;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.coordinate.model.Datacenter;
import com.ecwid.consul.v1.health.model.HealthService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Consul 同步 Nacos
 *
 * @author paderlol
 * @date: 2018-12-31 16:25
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.CONSUL, destinationCluster = ClusterTypeEnum.NACOS)
public class ConsulSyncToNacosServiceImpl implements SyncService {

    @Autowired
    private MetricsManager metricsManager;

    private final ConsulServerHolder consulServerHolder;
    private final SkyWalkerCacheServices skyWalkerCacheServices;

    private final NacosServerHolder nacosServerHolder;

    private final SpecialSyncEventBus specialSyncEventBus;

    @Autowired
    public ConsulSyncToNacosServiceImpl(ConsulServerHolder consulServerHolder,
                                        SkyWalkerCacheServices skyWalkerCacheServices, NacosServerHolder nacosServerHolder,
                                        SpecialSyncEventBus specialSyncEventBus) {
        this.consulServerHolder = consulServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.specialSyncEventBus = specialSyncEventBus;
    }

    @Override
    public boolean delete(TaskDO taskDO) {

        try {
            specialSyncEventBus.unsubscribe(taskDO);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName());
            for (Instance instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO)) {

                    destNamingService.deregisterInstance(taskDO.getServiceName(), instance.getIp(), instance.getPort());
                }
            }

        } catch (Exception e) {
            log.error("delete task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            ConsulClient consulClient = consulServerHolder.get(taskDO.getSourceClusterId(), null);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);

            Response<List<Datacenter>> datacenters = consulClient.getDatacenters();
            List allInstances = datacenters.getValue().stream().map(datacenter -> CompletableFuture.supplyAsync(() -> {
                List<HealthService> healthServiceList = null;
                try {
                    QueryParams queryParams = QueryParams.Builder.builder().setDatacenter(datacenter.getDatacenter()).build();
                    Response<List<HealthService>> response =
                            consulClient.getHealthServices(taskDO.getServiceName(), true, queryParams);
                    healthServiceList = response.getValue();

                } catch (Exception e) {
                    log.error("Get task from consul failed, taskId:{}, dataCenter:{}", taskDO.getTaskId(), datacenter, e);
                    metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                }
                return healthServiceList;
            })).map(future -> {
                try {
                    return future.get();
                } catch (Exception e) {
                    log.error("Call get method on CompletableFuture failed taskId:{}", taskDO.getTaskId(), e);
                }
                return Collections.emptyList();
            }).flatMap(Collection::stream).collect(Collectors.toList());

            Set<String> instanceKeys = new HashSet<>();
            for (Object obj : allInstances) {
                HealthService healthService = (HealthService) obj;
                if (needSync(ConsulUtils.transferMetadata(healthService.getService().getTags()))) {
                    destNamingService.registerInstance(taskDO.getServiceName(),
                            buildSyncInstance(healthService, taskDO));
                    instanceKeys.add(composeInstanceKey(healthService.getService().getAddress(),
                            healthService.getService().getPort()));
                }
            }
            List<Instance> originalInstances = destNamingService.getAllInstances(taskDO.getServiceName());
            for (Instance instance : originalInstances) {
                if (needDelete(instance.getMetadata(), taskDO)
                        && !instanceKeys.contains(composeInstanceKey(instance.getIp(), instance.getPort()))) {
                    destNamingService.deregisterInstance(taskDO.getServiceName(), instance.getIp(), instance.getPort());
                }
            }
            specialSyncEventBus.subscribe(taskDO, this::sync);
        } catch (Exception e) {
            log.error("Sync task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);

        }
        return true;
    }

    private Instance buildSyncInstance(HealthService instance, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(instance.getService().getAddress());
        temp.setPort(instance.getService().getPort());
        Map<String, String> metaData = new HashMap<>(ConsulUtils.transferMetadata(instance.getService().getTags()));
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }

    private String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }

}
