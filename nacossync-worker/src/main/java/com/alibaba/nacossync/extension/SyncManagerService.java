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
package com.alibaba.nacossync.extension;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.impl.extend.NacosSyncToZookeeperServicesSharding;
import com.alibaba.nacossync.extension.impl.extend.Sharding;
import com.alibaba.nacossync.pojo.model.TaskDO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacossync.util.SkyWalkerUtil.generateSyncKey;

/**
 * @author NacosSync
 * @version $Id: SyncManagerService.java, v 0.1 2018-09-25 PM5:17 NacosSync Exp $$
 */
@Slf4j
@Service
public class SyncManagerService implements InitializingBean, ApplicationContextAware {

    protected final SkyWalkerCacheServices skyWalkerCacheServices;

    private ConcurrentHashMap<String, SyncService> syncServiceMap = new ConcurrentHashMap<String, SyncService>();

    private ApplicationContext applicationContext;

    @Resource(type = NacosSyncToZookeeperServicesSharding.class)
    private Sharding sharding;

    public SyncManagerService(
            SkyWalkerCacheServices skyWalkerCacheServices) {
        this.skyWalkerCacheServices = skyWalkerCacheServices;
    }

    public boolean delete(TaskDO taskDO) throws NacosException {
        if (this.skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode().equals(ClusterTypeEnum.NACOS.getCode()) && this.skyWalkerCacheServices.getClusterType(taskDO.getDestClusterId()).getCode().equals(ClusterTypeEnum.ZK.getCode())) {
            sharding.stop(taskDO);
        }
        return getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId()).delete(taskDO);

    }

    public boolean sync(TaskDO taskDO) {
        if (this.skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode().equals(ClusterTypeEnum.NACOS.getCode()) && this.skyWalkerCacheServices.getClusterType(taskDO.getDestClusterId()).getCode().equals(ClusterTypeEnum.ZK.getCode())) {
            sharding.start(taskDO);
            return true;
        }
        return getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId()).sync(taskDO);

    }

    @Override
    public void afterPropertiesSet() {
        this.applicationContext.getBeansOfType(SyncService.class).forEach((key, value) -> {
            NacosSyncService nacosSyncService = value.getClass().getAnnotation(NacosSyncService.class);
            ClusterTypeEnum sourceCluster = nacosSyncService.sourceCluster();
            ClusterTypeEnum destinationCluster = nacosSyncService.destinationCluster();
            syncServiceMap.put(generateSyncKey(sourceCluster, destinationCluster), value);
        });
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public SyncService getSyncService(String sourceClusterId, String destClusterId) {

        ClusterTypeEnum sourceClusterType = this.skyWalkerCacheServices.getClusterType(sourceClusterId);
        ClusterTypeEnum destClusterType = this.skyWalkerCacheServices.getClusterType(destClusterId);

        return syncServiceMap.get(generateSyncKey(sourceClusterType, destClusterType));
    }

    public boolean syncChangeService(TaskDO taskDO, String serviceName) {
        TaskDO taskDO1 = new TaskDO();
        BeanUtils.copyProperties(taskDO, taskDO1);
        taskDO1.setTaskId(serviceName);//需要一个key替换以前的taskid很多封装维度，暂时使用serviceName
        taskDO1.setServiceName(serviceName);
        getSyncService(taskDO1.getSourceClusterId(), taskDO1.getDestClusterId()).sync(taskDO1);
        return true;
    }

    public boolean deleteChangeService(TaskDO taskDO, String serviceName) {
        TaskDO taskDO1 = new TaskDO();
        BeanUtils.copyProperties(taskDO, taskDO1);
        taskDO1.setTaskId(serviceName);
        taskDO1.setServiceName(serviceName);
        getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId()).delete(taskDO1);
        return true;
    }

}
