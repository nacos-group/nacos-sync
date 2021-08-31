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

import com.google.common.base.Joiner;
import java.util.List;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.stereotype.Service;

/**
 * @author paderlol
 * @date: 2018-12-24 22:07
 */
@Service
@Slf4j
public class ZookeeperServerHolder extends AbstractServerHolderImpl<CuratorFramework> {


    @Override
    CuratorFramework createServer(String clusterId, Supplier<String> serverAddressSupplier) {
        List<String> allClusterConnectKey = skyWalkerCacheServices
                .getAllClusterConnectKey(clusterId);
        String serverList = Joiner.on(",").join(allClusterConnectKey);
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(serverList)
                .retryPolicy(new RetryNTimes(1, 3000))
                .connectionTimeoutMs(5000);

        CuratorFramework client = builder.build();
        client.getConnectionStateListenable().addListener((clientInstance, state) -> {
            if (state == ConnectionState.LOST) {
                log.error("zk address: {} client state LOST",serverList);
            } else if (state == ConnectionState.CONNECTED) {
                log.info("zk address: {} client state CONNECTED",serverList);
            } else if (state == ConnectionState.RECONNECTED) {
                log.info("zk address: {} client state RECONNECTED",serverList);
            }
        });
        client.start();
        return client;
    }
}
