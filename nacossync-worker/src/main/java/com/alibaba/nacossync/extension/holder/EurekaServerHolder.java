package com.alibaba.nacossync.extension.holder;

import org.springframework.cloud.netflix.eureka.http.RestTemplateTransportClientFactory;
import org.springframework.stereotype.Service;

import com.netflix.discovery.shared.resolver.DefaultEndpoint;
import com.netflix.discovery.shared.resolver.EurekaEndpoint;
import com.netflix.discovery.shared.transport.EurekaHttpClient;

import lombok.extern.slf4j.Slf4j;

/**
 * @author paderlol
 * @date: 2018-12-31 16:26
 */
@Service
@Slf4j
public class EurekaServerHolder extends AbstractServerHolder<EurekaHttpClient>{
    @Override
    EurekaHttpClient createServer(String serverAddress, String namespace) {
        RestTemplateTransportClientFactory restTemplateTransportClientFactory =
                new RestTemplateTransportClientFactory();
        EurekaEndpoint eurekaEndpoint = new DefaultEndpoint(serverAddress);
        return restTemplateTransportClientFactory.newClient(eurekaEndpoint);
    }
}
