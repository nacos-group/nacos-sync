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

import com.alibaba.nacos.api.exception.NacosException;
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
import com.ecwid.consul.v1.health.model.HealthService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

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
            Response<List<HealthService>> response =
                consulClient.getHealthServices(taskDO.getServiceName(), true, QueryParams.DEFAULT);
            List<HealthService> healthServiceList = response.getValue();
            Set<String> instanceKeys = new HashSet<>();
            overrideAllInstance(taskDO, destNamingService, healthServiceList, instanceKeys);
            cleanAllOldInstance(taskDO, destNamingService, instanceKeys);
            specialSyncEventBus.subscribe(taskDO, this::sync);
        } catch (Exception e) {
            log.error("Sync task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private void cleanAllOldInstance(TaskDO taskDO, NamingService destNamingService, Set<String> instanceKeys)
        throws NacosException {
        List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName());
        for (Instance instance : allInstances) {
            if (needDelete(instance.getMetadata(), taskDO)
                && !instanceKeys.contains(composeInstanceKey(instance.getIp(), instance.getPort()))) {

                destNamingService.deregisterInstance(taskDO.getServiceName(), instance.getIp(), instance.getPort());
            }
        }
    }

    private void overrideAllInstance(TaskDO taskDO, NamingService destNamingService,
        List<HealthService> healthServiceList, Set<String> instanceKeys) throws NacosException {
        for (HealthService healthService : healthServiceList) {
            if (needSync(ConsulUtils.transferMetadata(healthService.getService().getTags()))) {
                destNamingService.registerInstance(taskDO.getServiceName(),
                    buildSyncInstance(healthService, taskDO));
                instanceKeys.add(composeInstanceKey(healthService.getService().getAddress(),
                    healthService.getService().getPort()));
            }
        }
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
