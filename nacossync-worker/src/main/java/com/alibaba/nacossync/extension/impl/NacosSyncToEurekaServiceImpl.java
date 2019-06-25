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
import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.LeaseInfo;
import com.netflix.appinfo.MyDataCenterInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author huangchen
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.EUREKA)
public class NacosSyncToEurekaServiceImpl implements SyncService {

    private final SpecialSyncEventBus specialSyncEventBus;
    private final MetricsManager metricsManager;
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    private final NacosServerHolder nacosServerHolder;
    private final EurekaServerHolder eurekaNamingServerHolder;

    public NacosSyncToEurekaServiceImpl(MetricsManager metricsManager, SkyWalkerCacheServices skyWalkerCacheServices,
                                        NacosServerHolder nacosServerHolder, SpecialSyncEventBus specialSyncEventBus,
                                        EurekaServerHolder eurekaNamingServerHolder) {
        this.metricsManager = metricsManager;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.specialSyncEventBus = specialSyncEventBus;
        this.eurekaNamingServerHolder = eurekaNamingServerHolder;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        try {

            specialSyncEventBus.unsubscribe(taskDO);
            EurekaNamingService eurekaNamingService = eurekaNamingServerHolder.get(taskDO.getDestClusterId(), null);
            // 删除目标集群中同步的实例列表
            List<InstanceInfo> allInstances = eurekaNamingService.getApplications(taskDO.getServiceName());
            if (allInstances != null) {
                for (InstanceInfo instance : allInstances) {
                    if (needDelete(instance.getMetadata(), taskDO)) {
                        eurekaNamingService.deregisterInstance(instance);
                    }
                }
            }

        } catch (Exception e) {
            log.error("delete task from nacos to eureka was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            EurekaNamingService destNamingService = eurekaNamingServerHolder.get(taskDO.getDestClusterId(), null);
            NamingService sourceNamingService = nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getNameSpace());
            List<Instance> instances = sourceNamingService.getAllInstances(taskDO.getServiceName());
            Set<String> instanceKeySet = new HashSet<>();
            //注册nacos上面的实例到eureka，并维持一个set列表
            for (Instance instance : instances) {
                if (needSync(instance.getMetadata())) {
                    instanceKeySet.add(composeInstanceKey(instance.getIp(), instance.getPort()));
                    destNamingService.registerInstance(buildSyncInstance(instance, taskDO));
                }
            }
            // 查询eureka上的实例列表与set里面做对比，删除不存在的实例
            List<InstanceInfo> destInstances = destNamingService.getApplications(taskDO.getServiceName());
            if (destInstances != null) {
                for (InstanceInfo instance : destInstances) {
                    if (needDelete(instance.getMetadata(), taskDO)
                            && !instanceKeySet.contains(composeInstanceKey(instance.getIPAddr(), instance.getPort()))) {
                        destNamingService.deregisterInstance(instance);
                    }
                }
            }
            specialSyncEventBus.subscribe(taskDO, this::sync);
        } catch (Exception e) {
            log.error("sync task from eureka to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }

    private InstanceInfo buildSyncInstance(Instance instance, TaskDO taskDO) {
        DataCenterInfo dataCenterInfo = new MyDataCenterInfo(DataCenterInfo.Name.MyOwn);
        HashMap<String, String> metadata = new HashMap<>(16);
        metadata.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metadata.put(SkyWalkerConstants.SYNC_SOURCE_KEY, skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        String homePageUrl = "http://" + instance.getIp() + ":" + instance.getPort();
        String serviceName = taskDO.getServiceName();

        return new InstanceInfo(
                instance.getIp() + ":" + serviceName + ":" + instance.getPort(),
                serviceName,
                null,
                instance.getIp(),
                null,
                new InstanceInfo.PortWrapper(true, instance.getPort()),
                null,
                homePageUrl,
                homePageUrl + "/info",
                homePageUrl + "/health",
                null,
                serviceName,
                serviceName,
                1,
                dataCenterInfo,
                instance.getIp(),
                InstanceInfo.InstanceStatus.UP,
                InstanceInfo.InstanceStatus.UNKNOWN,
                null,
                new LeaseInfo(30, 90,
                        0L, 0L, 0L, 0L, 0L),
                false,
                metadata,
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                null,
                null
        );
    }

}
