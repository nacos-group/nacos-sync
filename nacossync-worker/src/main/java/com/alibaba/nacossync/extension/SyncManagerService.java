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

import static com.alibaba.nacossync.util.SkyWalkerUtil.generateSyncKey;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.pojo.model.TaskDO;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

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

    public SyncManagerService(
        SkyWalkerCacheServices skyWalkerCacheServices) {
        this.skyWalkerCacheServices = skyWalkerCacheServices;
    }

    public boolean delete(TaskDO taskDO) throws NacosException {

        return getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId()).delete(taskDO);

    }

    public boolean sync(TaskDO taskDO) {

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

}
