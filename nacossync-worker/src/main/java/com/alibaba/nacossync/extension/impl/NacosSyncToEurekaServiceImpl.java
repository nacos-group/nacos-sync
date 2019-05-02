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
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.MyDataCenterInfo;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhanglong
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.EUREKA)
public class NacosSyncToEurekaServiceImpl implements SyncService {
    private Map<String, EventListener> nacosListenerMap = new ConcurrentHashMap<>();

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
            EurekaHttpClient eurekaHttpClient =
                eurekaServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());

            sourceNamingService.unsubscribe(taskDO.getServiceName(), nacosListenerMap.get(taskDO.getTaskId()));

            // 删除目标集群中同步的实例列表
            EurekaHttpResponse<Application> eurekaHttpResponse =
                eurekaHttpClient.getApplication(taskDO.getServiceName());
            if (Objects.requireNonNull(HttpStatus.resolve(eurekaHttpResponse.getStatusCode())).is2xxSuccessful()) {
                List<InstanceInfo> allInstances = eurekaHttpResponse.getEntity().getInstances();
                for (InstanceInfo instance : allInstances) {
                    if (needDelete(instance.getMetadata(), taskDO)) {
                        eurekaHttpClient.cancel(instance.getAppName(), instance.getId());
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
            EurekaHttpClient eurekaHttpClient =
                eurekaServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());

            nacosListenerMap.putIfAbsent(taskDO.getTaskId(), event -> {
                if (event instanceof NamingEvent) {
                    try {
                        Set<String> instanceKeySet = new HashSet();
                        List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName());
                        // 先将新的注册一遍
                        for (Instance instance : sourceInstances) {
                            if (needSync(instance.getMetadata())) {

                                eurekaHttpClient.register(buildSyncInstance(instance, taskDO));
                                instance.getInstanceId();
                                instanceKeySet.add(composeInstanceKey(instance.getIp(), instance.getPort()));
                            }
                        }
                        EurekaHttpResponse<Application> eurekaHttpResponse =
                                eurekaHttpClient.getApplication(taskDO.getServiceName());
                        if (Objects.requireNonNull(HttpStatus.resolve(eurekaHttpResponse.getStatusCode())).is2xxSuccessful()) {
                            List<InstanceInfo> allInstances = eurekaHttpResponse.getEntity().getInstances();
                            for (InstanceInfo instance : allInstances) {
                                if (needDelete(instance.getMetadata(), taskDO)&&!instanceKeySet.contains(composeInstanceKey(instance.getIPAddr(),
                                        instance.getPort()))) {
                                    eurekaHttpClient.cancel(instance.getAppName(), instance.getId());
                                }
                            }
                        }
                        // 再将不存在的删掉

                    } catch (Exception e) {
                        log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                    }
                }
            });

            sourceNamingService.subscribe(taskDO.getServiceName(), nacosListenerMap.get(taskDO.getTaskId()));
        } catch (Exception e) {
            log.error("sync task from nacos to eureka was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }

    public InstanceInfo buildSyncInstance(Instance instance, TaskDO taskDO) {
        DataCenterInfo dataCenterInfo = new MyDataCenterInfo(DataCenterInfo.Name.MyOwn);
        InstanceInfo instanceInfo = InstanceInfo.Builder.newBuilder().setAppName(taskDO.getServiceName())
            .setActionType(InstanceInfo.ActionType.ADDED).setIPAddr(instance.getIp()).setPort(instance.getPort())
            .setStatus(InstanceInfo.InstanceStatus.UP).add(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId())
                .setHostName(instance.getInstanceId()).setInstanceId(instance.getInstanceId())
                .setDataCenterInfo(dataCenterInfo)
                .add(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode())
            .add(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId()).build();

        return instanceInfo;
    }

}
