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
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.pojo.model.TaskDO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.nacossync.util.NacosUtils.getGroupNameOrDefault;

/**
 * @author yangyshdan
 * @version $Id: ConfigServerSyncManagerService.java, v 0.1 2018-11-12 下午5:17 NacosSync Exp $$
 */

@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.NACOS)
public class NacosSyncToNacosServiceImpl extends AbstractNacosSync {
    
    
    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;
    
    
    @Override
    public String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }
    
    @Override
    public void register(TaskDO taskDO, Instance instance) {
        NamingService destNamingService = getNacosServerHolder().get(taskDO.getDestClusterId());
        try {
            destNamingService.registerInstance(taskDO.getServiceName(), getGroupNameOrDefault(taskDO.getGroupName()),
                    buildSyncInstance(instance, taskDO));
        } catch (NacosException e) {
            log.error("Register instance={} to Nacos failed", taskDO.getServiceName(), e);
        }
    }
    
    @Override
    public void deregisterInstance(TaskDO taskDO) throws Exception {
        NamingService destNamingService = getNacosServerHolder().get(taskDO.getDestClusterId());
        List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName(),
                getGroupNameOrDefault(taskDO.getGroupName()), new ArrayList<>(), true);
        for (Instance instance : allInstances) {
            if (needDelete(instance.getMetadata(), taskDO)) {
                destNamingService.deregisterInstance(taskDO.getServiceName(),
                        getGroupNameOrDefault(taskDO.getGroupName()), instance.getIp(), instance.getPort());
            }
        }
    }
    
    @Override
    public void removeInvalidInstance(TaskDO taskDO, Set<String> invalidInstanceKeys) {
        NamingService destNamingService = getNacosServerHolder().get(taskDO.getDestClusterId());
        
        for (String instanceKey : invalidInstanceKeys) {
            String[] split = instanceKey.split(":", -1);
            try {
                destNamingService.deregisterInstance(taskDO.getServiceName(),
                        getGroupNameOrDefault(taskDO.getGroupName()), split[0], Integer.parseInt(split[1]));
            } catch (NacosException e) {
                log.error("Remove instance={} from Nacos failed", taskDO.getServiceName(), e);
                
            }
        }
    }
    
    
    private Instance buildSyncInstance(Instance instance, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(instance.getIp());
        temp.setPort(instance.getPort());
        temp.setClusterName(instance.getClusterName());
        temp.setServiceName(instance.getServiceName());
        temp.setEnabled(instance.isEnabled());
        temp.setHealthy(instance.isHealthy());
        temp.setWeight(instance.getWeight());
        temp.setEphemeral(instance.isEphemeral());
        Map<String, String> metaData = new HashMap<>(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }
    
    
}
