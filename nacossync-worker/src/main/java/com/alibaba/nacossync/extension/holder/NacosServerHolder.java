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
package com.alibaba.nacossync.extension.holder;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.client.naming.NacosNamingService;
import com.alibaba.nacos.client.naming.remote.NamingClientProxyDelegate;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientProxy;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.base.Joiner;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.stereotype.Service;
import org.springframework.util.ReflectionUtils;

/**
 * @author paderlol
 * @date: 2018-12-24 21:48
 */
@Service
@Slf4j
public class NacosServerHolder extends AbstractServerHolderImpl<NamingService> {

    private final ClusterAccessService clusterAccessService;
    
    private final TaskAccessService taskAccessService;
    
    private static ConcurrentHashMap<String,NamingService> globalNamingService = new ConcurrentHashMap<>(16);
    
    private static ConcurrentHashMap<String,NamingHttpClientProxy> globalNamingHttpProxy = new ConcurrentHashMap<>(16);
    
    private static ConcurrentHashMap<String,ConfigService> globalConfigService = new ConcurrentHashMap<>(16);

    public NacosServerHolder(ClusterAccessService clusterAccessService, TaskAccessService taskAccessService) {
        this.clusterAccessService = clusterAccessService;
        this.taskAccessService = taskAccessService;
    }

    @Override
    NamingService createServer(String clusterId, Supplier<String> serverAddressSupplier)
        throws Exception {
        String newClusterId;
        if (clusterId.contains(":")) {
            String[] split = clusterId.split(":");
            newClusterId = split[1];
        } else {
            newClusterId = clusterId;
        }
        //代表此时为组合key，确定target集群中的nameService是不同的
        List<String> allClusterConnectKey = skyWalkerCacheServices
            .getAllClusterConnectKey(newClusterId);
        ClusterDO clusterDO = clusterAccessService.findByClusterId(newClusterId);
        String serverList = Joiner.on(",").join(allClusterConnectKey);
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverList);
        Optional.ofNullable(clusterDO.getUserName()).ifPresent(value ->
            properties.setProperty(PropertyKeyConst.USERNAME, value)
        );

        Optional.ofNullable(clusterDO.getPassword()).ifPresent(value ->
            properties.setProperty(PropertyKeyConst.PASSWORD, value)
        );
        
        // configService不能设置namespace,否则获取不到dubbo服务元数据
        globalConfigService.computeIfAbsent(newClusterId, id -> {
            try {
                return ConfigFactory.createConfigService(properties);
            } catch (NacosException e) {
                log.error("start config service fail,clusterId:{}", id, e);
                return null;
            }
        });
        
        properties.setProperty(PropertyKeyConst.NAMESPACE, Optional.ofNullable(clusterDO.getNamespace()).orElse(
                Strings.EMPTY));
        return globalNamingService.computeIfAbsent(newClusterId, id -> {
            try {
                NacosNamingService namingService = (NacosNamingService) NamingFactory.createNamingService(properties);
                
                // clientProxy
                final Field clientProxyField = ReflectionUtils.findField(NacosNamingService.class, "clientProxy");
                assert clientProxyField != null;
                ReflectionUtils.makeAccessible(clientProxyField);
                NamingClientProxyDelegate clientProxy = (NamingClientProxyDelegate) ReflectionUtils.getField(clientProxyField, namingService);
                
                // httpClientProxy
                final Field httpClientProxyField = ReflectionUtils.findField(NamingClientProxyDelegate.class, "httpClientProxy");
                assert httpClientProxyField != null;
                ReflectionUtils.makeAccessible(httpClientProxyField);
                NamingHttpClientProxy httpClientProxy = (NamingHttpClientProxy) ReflectionUtils.getField(httpClientProxyField, clientProxy);
                globalNamingHttpProxy.put(id, httpClientProxy);
                
                return namingService;
            } catch (NacosException e) {
                log.error("start naming service fail,clusterId:{}", id, e);
                return null;
            }
        });
    }
    
    /**
     * Get NamingService for different clients
     * @param clusterId clusterId
     * @return Returns Naming Service objects for different clusters
     */
    public NamingService getNamingService(String clusterId){
        return globalNamingService.get(clusterId);
    }

    public NamingHttpClientProxy getNamingHttpProxy(String clusterId){
        return globalNamingHttpProxy.get(clusterId);
    }

    public ConfigService getConfigService(String clusterId) {
        return globalConfigService.get(clusterId);
    }

    public NamingService getSourceNamingService(String taskId, String sourceClusterId) {
        String key = taskId + sourceClusterId;
        return serviceMap.computeIfAbsent(key, k->{
            try {
                log.info("Starting create source cluster server, key={}", key);
                //代表此时为组合key，确定target集群中的nameService是不同的
                List<String> allClusterConnectKey = skyWalkerCacheServices
                        .getAllClusterConnectKey(sourceClusterId);
                ClusterDO clusterDO = clusterAccessService.findByClusterId(sourceClusterId);
                TaskDO task = taskAccessService.findByTaskId(taskId);
                String serverList = Joiner.on(",").join(allClusterConnectKey);
                Properties properties = new Properties();
                properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverList);
                properties.setProperty(PropertyKeyConst.NAMESPACE, Optional.ofNullable(clusterDO.getNamespace()).orElse(
                        Strings.EMPTY));
                Optional.ofNullable(clusterDO.getUserName()).ifPresent(value ->
                        properties.setProperty(PropertyKeyConst.USERNAME, value)
                );
        
                Optional.ofNullable(clusterDO.getPassword()).ifPresent(value ->
                        properties.setProperty(PropertyKeyConst.PASSWORD, value)
                );
                properties.setProperty(SkyWalkerConstants.SOURCE_CLUSTERID_KEY,task.getSourceClusterId());
                properties.setProperty(SkyWalkerConstants.DEST_CLUSTERID_KEY,task.getDestClusterId());
                return NamingFactory.createNamingService(properties);
            }catch (NacosException e) {
                log.error("start source server fail,taskId:{},sourceClusterId:{}"
                        , taskId, sourceClusterId, e);
                return null;
            }
        });
        
    }
}
