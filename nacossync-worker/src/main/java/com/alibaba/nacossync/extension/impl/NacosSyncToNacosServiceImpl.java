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

import com.alibaba.nacos.api.common.Constants;
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
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yangyshdan
 * @version $Id: ConfigServerSyncManagerService.java, v 0.1 2018-11-12 下午5:17 NacosSync Exp $$
 */

@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.NACOS)
public class NacosSyncToNacosServiceImpl implements SyncService {
    
    private Map<String, EventListener> nacosListenerMap = new ConcurrentHashMap<>();
    
    @Autowired
    private MetricsManager metricsManager;
    
    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;
    
    @Autowired
    private NacosServerHolder nacosServerHolder;
    
    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            String groupName = Optional.ofNullable(taskDO.getGroupName()).orElse(Constants.DEFAULT_GROUP);
            taskDO.setGroupName(groupName);
            
            NamingService sourceNamingService = nacosServerHolder
                    .get(taskDO.getSourceClusterId(), taskDO.getNameSpace());
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), taskDO.getNameSpace());
            
            sourceNamingService.unsubscribe(taskDO.getServiceName(), taskDO.getGroupName(),
                    nacosListenerMap.get(taskDO.getTaskId()));
            
            // 删除目标集群中同步的实例列表
            List<Instance> instances = destNamingService
                    .getAllInstances(taskDO.getServiceName(), taskDO.getGroupName());
            for (Instance instance : instances) {
                if (needDelete(instance.getMetadata(), taskDO)) {
                    destNamingService
                            .deregisterInstance(taskDO.getServiceName(), taskDO.getGroupName(), instance.getIp(),
                                    instance.getPort());
                }
            }
        } catch (Exception e) {
            log.error("delete task from nacos to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }
    
    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            NamingService sourceNamingService = nacosServerHolder
                    .get(taskDO.getSourceClusterId(), taskDO.getNameSpace());
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), taskDO.getNameSpace());
            
            String groupName = Optional.ofNullable(taskDO.getGroupName()).orElse(Constants.DEFAULT_GROUP);
            taskDO.setGroupName(groupName);
            nacosListenerMap.putIfAbsent(taskDO.getTaskId(), event -> {
                if (event instanceof NamingEvent) {
                    try {
                        Set instanceKeySet = new HashSet();
                        List<Instance> sourceInstances = sourceNamingService
                                .getAllInstances(taskDO.getServiceName(), taskDO.getGroupName());
                        // 先将新的注册一遍
                        for (Instance instance : sourceInstances) {
                            if (needSync(instance.getMetadata())) {
                                destNamingService.registerInstance(taskDO.getServiceName(), taskDO.getGroupName(),
                                        buildSyncInstance(instance, taskDO));
                                instanceKeySet.add(composeInstanceKey(instance));
                            }
                        }
                        
                        // 再将不存在的删掉
                        List<Instance> destInstances = destNamingService
                                .getAllInstances(taskDO.getServiceName(), taskDO.getGroupName());
                        for (Instance instance : destInstances) {
                            if (needDelete(instance.getMetadata(), taskDO) && !instanceKeySet
                                    .contains(composeInstanceKey(instance))) {
                                destNamingService.deregisterInstance(taskDO.getServiceName(), taskDO.getGroupName(),
                                        instance.getIp(), instance.getPort());
                            }
                        }
                    } catch (Exception e) {
                        log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                        metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                    }
                }
            });
            
            sourceNamingService.subscribe(taskDO.getServiceName(), taskDO.getGroupName(),
                    nacosListenerMap.get(taskDO.getTaskId()));
        } catch (Exception e) {
            log.error("sync task from nacos to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }
    
    private String composeInstanceKey(Instance instance) {
        return instance.getIp() + ":" + instance.getPort();
    }
    
    
    public Instance buildSyncInstance(Instance instance, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(instance.getIp());
        temp.setPort(instance.getPort());
        temp.setClusterName(instance.getClusterName());
        temp.setServiceName(instance.getServiceName());
        temp.setEnabled(instance.isEnabled());
        temp.setHealthy(instance.isHealthy());
        temp.setWeight(instance.getWeight());
        
        Map<String, String> metaData = new HashMap<>();
        metaData.putAll(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }
}
