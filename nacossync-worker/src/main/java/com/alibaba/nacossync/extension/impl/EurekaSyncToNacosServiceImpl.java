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
import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.netflix.appinfo.InstanceInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * eureka
 * 
 * @author paderlol
 * @date: 2018-12-31 16:25
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.EUREKA, destinationCluster = ClusterTypeEnum.NACOS)
public class EurekaSyncToNacosServiceImpl implements SyncService {

    private final MetricsManager metricsManager;

    private final EurekaServerHolder eurekaServerHolder;
    private final SkyWalkerCacheServices skyWalkerCacheServices;

    private final NacosServerHolder nacosServerHolder;

    private final SpecialSyncEventBus specialSyncEventBus;

    @Autowired
    public EurekaSyncToNacosServiceImpl(EurekaServerHolder eurekaServerHolder,
        SkyWalkerCacheServices skyWalkerCacheServices, NacosServerHolder nacosServerHolder,
        SpecialSyncEventBus specialSyncEventBus, MetricsManager metricsManager) {
        this.eurekaServerHolder = eurekaServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.specialSyncEventBus = specialSyncEventBus;
        this.metricsManager = metricsManager;
    }

    @Override
    public boolean delete(TaskDO taskDO) {

        try {
            specialSyncEventBus.unsubscribe(taskDO);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), taskDO.getNameSpace());
            List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName());
            deleteAllInstance(taskDO, destNamingService, allInstances);

        } catch (Exception e) {
            log.error("delete a task from eureka to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            EurekaNamingService eurekaNamingService = eurekaServerHolder.get(taskDO.getSourceClusterId(), null);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            List<InstanceInfo> instanceInfos = eurekaNamingService.getApplications(taskDO.getServiceName());
            List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName());

            if (Objects.nonNull(instanceInfos)) {
                for (InstanceInfo instanceInfo : instanceInfos) {
                    if (needSync(instanceInfo.getMetadata())) {
                        if (CollectionUtils.isEmpty(allInstances)
                            || isExistInNacosInstance(allInstances, instanceInfo)) {
                            destNamingService.registerInstance(taskDO.getServiceName(),
                                buildSyncInstance(instanceInfo, taskDO));
                        } else {
                            log.info("Remove invalid service instance from Nacos, serviceName={}, Ip={}, port={}",
                                instanceInfo.getAppName(), instanceInfo.getIPAddr(), instanceInfo.getPort());
                            destNamingService.deregisterInstance(instanceInfo.getAppName(), instanceInfo.getIPAddr(),
                                instanceInfo.getPort());
                        }
                    }

                }
            } else {
                deleteAllInstance(taskDO, destNamingService, allInstances);
            }
            specialSyncEventBus.subscribe(taskDO, this::sync);
        } catch (Exception e) {
            log.error("sync task from eureka to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private boolean isExistInNacosInstance(List<Instance> allInstances, InstanceInfo instanceInfo) {
        return allInstances.stream().anyMatch(instance -> instance.getIp().equals(instanceInfo.getIPAddr())
            && instance.getPort() == instanceInfo.getPort());
    }

    private void deleteAllInstance(TaskDO taskDO, NamingService destNamingService, List<Instance> allInstances)
        throws NacosException {
        for (Instance instance : allInstances) {
            if (needDelete(instance.getMetadata(), taskDO)) {
                destNamingService.deregisterInstance(taskDO.getServiceName(), instance);
            }

        }
    }

    private Instance buildSyncInstance(InstanceInfo instance, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(instance.getIPAddr());
        temp.setPort(instance.getPort());
        temp.setServiceName(instance.getAppName());
        temp.setHealthy(true);

        Map<String, String> metaData = new HashMap<>(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }

}
