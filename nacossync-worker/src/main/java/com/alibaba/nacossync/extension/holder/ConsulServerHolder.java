package com.alibaba.nacossync.extension.holder;

import com.ecwid.consul.v1.ConsulClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.URL;

/**
 * @author paderlol
 * @date: 2018-12-31 16:26
 */
@Service
@Slf4j
public class ConsulServerHolder extends AbstractServerHolder<ConsulClient> {

    public static final String HTTP = "http://";

    @Override
    ConsulClient createServer(String serverAddress, String namespace) throws Exception {
        serverAddress = serverAddress.startsWith(HTTP) ? serverAddress : HTTP + serverAddress;
        URL url = new URL(serverAddress);
        return new ConsulClient(url.getHost(), url.getPort());
    }

}
