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
