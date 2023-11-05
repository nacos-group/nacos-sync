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
import com.alibaba.nacos.common.utils.CollectionUtils;
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
import com.alibaba.nacossync.util.NacosUtils;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.model.HealthService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

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

            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());
            String servideName = taskDO.getServiceName();
            String groupName = NacosUtils.getGroupNameOrDefault(taskDO.getGroupName());
            List<Instance> allInstances = destNamingService.getAllInstances(servideName, groupName);
            List<Instance> needDeregisterInstances = new ArrayList<>();
            for (Instance instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO)) {
                    NacosSyncToNacosServiceImpl.removeUnwantedAttrsForNacosRedo(instance);
                    log.debug("需要反注册的实例: {}", instance);
                    needDeregisterInstances.add(instance);
                }
            }
            if (CollectionUtils.isNotEmpty(needDeregisterInstances)) {
                NacosSyncToNacosServiceImpl.doDeregisterInstance(taskDO, destNamingService, servideName, groupName, needDeregisterInstances);
            }
        } catch (Exception e) {
            log.error("delete task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO, Integer index) {
        try {
            ConsulClient consulClient = consulServerHolder.get(taskDO.getSourceClusterId());
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());
            Response<List<HealthService>> response =
                consulClient.getHealthServices(taskDO.getServiceName(), true, QueryParams.DEFAULT);
            List<HealthService> healthServiceList = response.getValue();
            Set<String> instanceKeys = new HashSet<>();
            overrideAllInstance(taskDO, destNamingService, healthServiceList, instanceKeys);
            cleanAllOldInstance(taskDO, destNamingService, instanceKeys);
            specialSyncEventBus.subscribe(taskDO, t->sync(t, index));
        } catch (Exception e) {
            log.error("Sync task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private void cleanAllOldInstance(TaskDO taskDO, NamingService destNamingService, Set<String> instanceKeys)
        throws NacosException {
        String serviceName = taskDO.getServiceName();
        String groupName = NacosUtils.getGroupNameOrDefault(taskDO.getGroupName());
        List<Instance> allInstances = destNamingService.getAllInstances(serviceName, groupName);
        List<Instance> needDeregisterInstances = new ArrayList<>();
        for (Instance instance : allInstances) {
            if (needDelete(instance.getMetadata(), taskDO)
                && !instanceKeys.contains(composeInstanceKey(instance.getIp(), instance.getPort()))) {
                NacosSyncToNacosServiceImpl.removeUnwantedAttrsForNacosRedo(instance);
                log.debug("需要反注册的实例: {}", instance);
                needDeregisterInstances.add(instance);
            }
        }
        if (CollectionUtils.isNotEmpty(needDeregisterInstances)) {
            NacosSyncToNacosServiceImpl.doDeregisterInstance(taskDO, destNamingService, serviceName, groupName, needDeregisterInstances);
        }
    }

    private void overrideAllInstance(TaskDO taskDO, NamingService destNamingService,
        List<HealthService> healthServiceList, Set<String> instanceKeys) throws NacosException {
        String serviceName = taskDO.getServiceName();
        String groupName = NacosUtils.getGroupNameOrDefault(taskDO.getGroupName());
        List<Instance> needRegisterInstances = new ArrayList<>();
        for (HealthService healthService : healthServiceList) {
            if (needSync(ConsulUtils.transferMetadata(healthService.getService().getTags()))) {
                Instance syncInstance = buildSyncInstance(healthService, taskDO);
                log.debug("需要从源集群同步到目标集群的实例：{}", syncInstance);
                needRegisterInstances.add(syncInstance);
                instanceKeys.add(composeInstanceKey(healthService.getService().getAddress(),
                        healthService.getService().getPort()));
            }
        }
        if (CollectionUtils.isNotEmpty(needRegisterInstances)) {
            if (needRegisterInstances.get(0).isEphemeral()) {
                //批量注册
                log.debug("将源集群指定service的临时实例全量同步到目标集群: {}", taskDO);
                destNamingService.batchRegisterInstance(serviceName, groupName, needRegisterInstances);
            } else {
                for (Instance instance : needRegisterInstances) {
                    log.debug("从源集群同步到目标集群的持久实例：{}", instance);
                    destNamingService.registerInstance(serviceName, groupName, instance);
                }
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
