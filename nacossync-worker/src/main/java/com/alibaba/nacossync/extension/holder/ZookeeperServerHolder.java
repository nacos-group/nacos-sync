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
public class ZookeeperServerHolder extends AbstractServerHolder<CuratorFramework> {


    @Override
    CuratorFramework createServer(String serverAddress, String namespace) {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(serverAddress)
                .retryPolicy(new RetryNTimes(1, 1000))
                .connectionTimeoutMs(5000);
        CuratorFramework client = builder.build();
        client.getConnectionStateListenable().addListener((client1, state) -> {
            if (state == ConnectionState.LOST) {
                log.error("zk address: {} client state LOST",serverAddress);
            } else if (state == ConnectionState.CONNECTED) {
                log.info("zk address: {} client state CONNECTED",serverAddress);
            } else if (state == ConnectionState.RECONNECTED) {
                log.info("zk address: {} client state RECONNECTED",serverAddress);
            }
        });
        client.start();
        return client;
    }
}
