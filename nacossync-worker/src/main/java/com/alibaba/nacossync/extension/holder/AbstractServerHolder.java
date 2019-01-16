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
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author paderlol
 * @date: 2018-12-24 22:08
 */
public abstract class AbstractServerHolder<T> implements Holder {
    private final ReentrantLock serviceMapLock = new ReentrantLock();
    private Map<String, T> serviceMap = new ConcurrentHashMap<>();
    @Autowired
    protected SkyWalkerCacheServices skyWalkerCacheServices;

    @Override
    public T get(String clusterId, String namespace) throws Exception {
        if (namespace == null) {
            namespace = "";
        }
        String key = clusterId + "_" + namespace;
        serviceMapLock.lock();
        try {
            if (serviceMap.get(key) == null) {
                serviceMap.put(key, createServer(skyWalkerCacheServices.getClusterConnectKey(clusterId), namespace));
            }
        } finally {
            serviceMapLock.unlock();
        }
        return serviceMap.get(key);
    }

     abstract T createServer(String serverAddress, String namespace) throws Exception;
}