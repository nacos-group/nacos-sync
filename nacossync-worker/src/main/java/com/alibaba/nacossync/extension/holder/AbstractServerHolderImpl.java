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

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * @author paderlol
 * @date 2018-12-24 22:08
 */
@Slf4j
public abstract class AbstractServerHolderImpl<T> implements Holder<T> {

    protected final Map<String, T> serviceMap = new ConcurrentHashMap<>();
    
    @Autowired
    protected SkyWalkerCacheServices skyWalkerCacheServices;

    @Override
    public T get(String clusterId) {

        return serviceMap.computeIfAbsent(clusterId, clusterKey -> {
            try {
                log.info("Starting create cluster server, clusterId={}", clusterId);
                return createServer(clusterId, () -> skyWalkerCacheServices.getClusterConnectKey(clusterId));
            } catch (Exception e) {
                log.error(String.format("clusterId=%s, start server failed", clusterId), e);
                return null;
            }
        });
    }

    /**
     * Create real cluster client instance
     *
     * @param clusterId             cluster id
     * @param serverAddressSupplier server address
     * @return cluster client instance
     */
    abstract T createServer(String clusterId, Supplier<String> serverAddressSupplier)
        throws Exception;

}
