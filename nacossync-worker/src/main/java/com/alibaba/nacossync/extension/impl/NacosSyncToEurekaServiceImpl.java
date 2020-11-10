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
import com.alibaba.nacos.api.naming.listener.Event;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.LeaseInfo;
import com.netflix.appinfo.MyDataCenterInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhanglong
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.EUREKA)
public class NacosSyncToEurekaServiceImpl implements SyncService {
    private final Map<String, EventListener> nacosListenerMap = new ConcurrentHashMap<>();

    private final MetricsManager metricsManager;
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    private final NacosServerHolder nacosServerHolder;
    private final EurekaServerHolder eurekaServerHolder;

    public NacosSyncToEurekaServiceImpl(MetricsManager metricsManager, SkyWalkerCacheServices skyWalkerCacheServices,
                                        NacosServerHolder nacosServerHolder, EurekaServerHolder eurekaServerHolder) {
        this.metricsManager = metricsManager;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.eurekaServerHolder = eurekaServerHolder;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            NamingService sourceNamingService =
                    nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());
            EurekaNamingService destNamingService =
                    eurekaServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());

            sourceNamingService.unsubscribe(taskDO.getServiceName(), nacosListenerMap.get(taskDO.getTaskId()));
            // 删除目标集群中同步的实例列表
            List<InstanceInfo> allInstances = destNamingService.getApplications(taskDO.getServiceName());
            if (allInstances != null) {
                for (InstanceInfo instance : allInstances) {
                    if (needDelete(instance.getMetadata(), taskDO)) {
                        destNamingService.deregisterInstance(instance);
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
            NamingService sourceNamingService =
                    nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());
            EurekaNamingService destNamingService =
                    eurekaServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());

            nacosListenerMap.putIfAbsent(taskDO.getTaskId(), event -> {
                processNamingEvent(taskDO, sourceNamingService, destNamingService, event);
            });

            sourceNamingService.subscribe(taskDO.getServiceName(), nacosListenerMap.get(taskDO.getTaskId()));
        } catch (Exception e) {
            log.error("sync task from eureka to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private void processNamingEvent(TaskDO taskDO, NamingService sourceNamingService,
        EurekaNamingService destNamingService, Event event) {
        if (event instanceof NamingEvent) {
            try {
                Set<String> instanceKeySet = new HashSet<>();
                List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName());
                // 先将新的注册一遍
                addAllNewInstance(taskDO, destNamingService, instanceKeySet, sourceInstances);
                // 再将不存在的删掉
                ifNecessaryDelete(taskDO, destNamingService, instanceKeySet);
            } catch (Exception e) {
                log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            }
        }
    }

    private void ifNecessaryDelete(TaskDO taskDO, EurekaNamingService destNamingService, Set<String> instanceKeySet) {
        List<InstanceInfo> allInstances = destNamingService.getApplications(taskDO.getServiceName());
        if (allInstances != null){
            for (InstanceInfo instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO) && !instanceKeySet.contains(composeInstanceKey(instance.getIPAddr(),
                        instance.getPort()))) {
                    destNamingService.deregisterInstance(instance);
                }

            }
        }
    }

    private void addAllNewInstance(TaskDO taskDO, EurekaNamingService destNamingService, Set<String> instanceKeySet,
        List<Instance> sourceInstances) {
        for (Instance instance : sourceInstances) {
            if (needSync(instance.getMetadata())) {
                destNamingService.registerInstance(buildSyncInstance(instance, taskDO));
                instanceKeySet.add(composeInstanceKey(instance.getIp(), instance.getPort()));
            }
        }
    }

    private String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }

    private InstanceInfo buildSyncInstance(Instance instance, TaskDO taskDO) {
        DataCenterInfo dataCenterInfo = new MyDataCenterInfo(DataCenterInfo.Name.MyOwn);
        final Map<String, String> instanceMetadata = instance.getMetadata();
        HashMap<String, String> metadata = new HashMap<>(16);
        metadata.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metadata.put(SkyWalkerConstants.SYNC_SOURCE_KEY, skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        metadata.putAll(instanceMetadata);
        String homePageUrl = obtainHomePageUrl(instance, instanceMetadata);
        String serviceName = taskDO.getServiceName();

        return new InstanceInfo(
                instance.getIp() + ":" + serviceName + ":" + instance.getPort(),
                serviceName,
                null,
                instance.getIp(),
                null,
                new InstanceInfo.PortWrapper(true, instance.getPort()),
                null,
                homePageUrl+"/actuator/env",
                homePageUrl + "/actuator/info",
                homePageUrl + "/actuator/health",
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

    private String obtainHomePageUrl(Instance instance, Map<String, String> instanceMetadata) {
        final String managementContextPath =
            obtainManagementContextPath(instanceMetadata);
        final String managementPort = instanceMetadata.getOrDefault(SkyWalkerConstants.MANAGEMENT_PORT_KEY,
            String.valueOf(instance.getPort()));
        return String.format("http://%s:%s%s",instance.getIp(),managementPort,managementContextPath);
    }

    private String obtainManagementContextPath(Map<String, String> instanceMetadata) {
        final String path = instanceMetadata.getOrDefault(SkyWalkerConstants.MANAGEMENT_CONTEXT_PATH_KEY, "");
        if (path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }



}
