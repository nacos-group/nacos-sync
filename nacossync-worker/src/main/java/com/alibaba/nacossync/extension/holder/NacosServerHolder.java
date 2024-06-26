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
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * @author paderlol
 * @date: 2018-12-24 21:48
 */
@Service
@Slf4j
public class NacosServerHolder extends AbstractServerHolderImpl<NamingService> {

    private final ClusterAccessService clusterAccessService;
    
    public NacosServerHolder(ClusterAccessService clusterAccessService) {
        this.clusterAccessService = clusterAccessService;
    }

    @Override
    NamingService createServer(String clusterId, Supplier<String> serverAddressSupplier)
        throws Exception {
        List<String> allClusterConnectKey = skyWalkerCacheServices
            .getAllClusterConnectKey(clusterId);
        ClusterDO clusterDO = clusterAccessService.findByClusterId(clusterId);
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
        return NamingFactory.createNamingService(properties);
    }
    
}
