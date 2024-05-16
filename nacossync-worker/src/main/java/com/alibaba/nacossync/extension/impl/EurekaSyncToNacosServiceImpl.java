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
import com.alibaba.nacossync.util.NacosUtils;
import com.netflix.appinfo.InstanceInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

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

            EurekaNamingService eurekaNamingService = eurekaServerHolder.get(taskDO.getSourceClusterId());
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());

            List<InstanceInfo> eurekaInstances = eurekaNamingService.getApplications(taskDO.getServiceName());
            deleteAllInstanceFromEureka(taskDO, destNamingService, eurekaInstances);

        } catch (Exception e) {
            log.error("delete a task from eureka to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO,Integer index) {
        try {

            EurekaNamingService eurekaNamingService = eurekaServerHolder.get(taskDO.getSourceClusterId());
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());

            List<InstanceInfo> eurekaInstances = eurekaNamingService.getApplications(taskDO.getServiceName());
            List<Instance> nacosInstances = destNamingService.getAllInstances(taskDO.getServiceName(),
                NacosUtils.getGroupNameOrDefault(taskDO.getGroupName()));

            if (CollectionUtils.isEmpty(eurekaInstances)) {
                // Clear all instance from Nacos
                deleteAllInstance(taskDO, destNamingService, nacosInstances);
            } else {
                if (!CollectionUtils.isEmpty(nacosInstances)) {
                    // Remove invalid instance from Nacos
                    removeInvalidInstance(taskDO, destNamingService, eurekaInstances, nacosInstances);
                }
                addValidInstance(taskDO, destNamingService, eurekaInstances);
            }
            specialSyncEventBus.subscribe(taskDO, t->sync(t, index));
        } catch (Exception e) {
            log.error("sync task from eureka to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }

    private void addValidInstance(TaskDO taskDO, NamingService destNamingService, List<InstanceInfo> eurekaInstances)
        throws NacosException {
        String serviceName = taskDO.getServiceName();
        String groupName = NacosUtils.getGroupNameOrDefault(taskDO.getGroupName());
        List<Instance> needRegisterInstances = new ArrayList<>();
        for (InstanceInfo instance : eurekaInstances) {
            if (needSync(instance.getMetadata())) {
                log.info("Add service instance from Eureka, serviceName={}, Ip={}, port={}",
                        instance.getAppName(), instance.getIPAddr(), instance.getPort());
                Instance syncInstance = buildSyncInstance(instance, taskDO);
                log.debug("需要从源集群同步到目标集群的实例：{}", syncInstance);
                needRegisterInstances.add(syncInstance);
            }
        }
        if (CollectionUtils.isEmpty(needRegisterInstances)) {
            return;
        }
        if (needRegisterInstances.get(0).isEphemeral()) {
            // 批量注册
            log.debug("将源集群指定service的临时实例全量同步到目标集群: {}", taskDO);
            destNamingService.batchRegisterInstance(serviceName, groupName, needRegisterInstances);
        } else {
            for (Instance instance : needRegisterInstances) {
                log.debug("从源集群同步到目标集群的持久实例：{}", instance);
                destNamingService.registerInstance(serviceName, groupName, instance);
            }
        }
    }

    private void deleteAllInstanceFromEureka(TaskDO taskDO, NamingService destNamingService,
        List<InstanceInfo> eurekaInstances)
        throws NacosException {
        if (CollectionUtils.isEmpty(eurekaInstances)) {
            return;
        }
        List<Instance> needDeregisterInstances = new ArrayList<>();
        for (InstanceInfo instance : eurekaInstances) {
            if (needSync(instance.getMetadata())) {
                log.info("Delete service instance from Eureka, serviceName={}, Ip={}, port={}",
                    instance.getAppName(), instance.getIPAddr(), instance.getPort());
                Instance needDeregisterInstance = buildSyncInstance(instance, taskDO);
                log.debug("需要反注册的实例: {}", needDeregisterInstance);
                needDeregisterInstances.add(needDeregisterInstance);
            }
        }
        if (CollectionUtils.isEmpty(needDeregisterInstances)) {
            return;
        }
        NacosSyncToNacosServiceImpl.doDeregisterInstance(taskDO, destNamingService, taskDO.getServiceName(),
                NacosUtils.getGroupNameOrDefault(taskDO.getGroupName()), needDeregisterInstances);
    }

    private void removeInvalidInstance(TaskDO taskDO, NamingService destNamingService,
        List<InstanceInfo> eurekaInstances, List<Instance> nacosInstances) throws NacosException {
        List<Instance> needDeregisterInstances = new ArrayList<>();
        for (Instance instance : nacosInstances) {
            if (!isExistInEurekaInstance(eurekaInstances, instance) && needDelete(instance.getMetadata(), taskDO)) {
                log.info("Remove invalid service instance from Nacos, serviceName={}, Ip={}, port={}",
                    instance.getServiceName(), instance.getIp(), instance.getPort());
                NacosSyncToNacosServiceImpl.removeUnwantedAttrsForNacosRedo(instance);
                log.debug("需要反注册的实例: {}", instance);
                needDeregisterInstances.add(instance);
            }
        }
        if (CollectionUtils.isEmpty(needDeregisterInstances)) {
            return;
        }
        NacosSyncToNacosServiceImpl.doDeregisterInstance(taskDO, destNamingService, taskDO.getServiceName(),
                NacosUtils.getGroupNameOrDefault(taskDO.getGroupName()), needDeregisterInstances);
    }

    private boolean isExistInEurekaInstance(List<InstanceInfo> eurekaInstances, Instance nacosInstance) {

        return eurekaInstances.stream().anyMatch(instance -> instance.getIPAddr().equals(nacosInstance.getIp())
            && instance.getPort() == nacosInstance.getPort());
    }

    private void deleteAllInstance(TaskDO taskDO, NamingService destNamingService, List<Instance> allInstances)
        throws NacosException {
        List<Instance> needDeregisterInstances = new ArrayList<>();
        for (Instance instance : allInstances) {
            if (needDelete(instance.getMetadata(), taskDO)) {
                NacosSyncToNacosServiceImpl.removeUnwantedAttrsForNacosRedo(instance);
                log.debug("需要反注册的实例: {}", instance);
                needDeregisterInstances.add(instance);
            }
        }
        if (CollectionUtils.isEmpty(needDeregisterInstances)) {
            return;
        }
        NacosSyncToNacosServiceImpl.doDeregisterInstance(taskDO, destNamingService, taskDO.getServiceName(),
                NacosUtils.getGroupNameOrDefault(taskDO.getGroupName()), needDeregisterInstances);
    }

    private Instance buildSyncInstance(InstanceInfo instance, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(instance.getIPAddr());
        temp.setPort(instance.getPort());
        //查询nacos集群实例返回的serviceName含组名前缀，但Nacos2服务端检查批量注册请求serviceName参数时不能包含组名前缀，因此注册实例到目标集群时不再设置serviceName。
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
