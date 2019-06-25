package com.alibaba.nacossync.extension.holder;

import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.netflix.discovery.shared.resolver.DefaultEndpoint;
import com.netflix.discovery.shared.resolver.EurekaEndpoint;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.netflix.eureka.http.RestTemplateTransportClientFactory;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;

/**
 * @author: Liu Jun Jie
 * @create: 2019-06-25 14:26
 **/
@Service
@Slf4j
public class EurekaServerHolder extends AbstractServerHolder<EurekaNamingService> {
    @Override
    EurekaNamingService createServer(String clusterId, Supplier<String> serverAddressSupplier, String namespace) throws Exception {
        RestTemplateTransportClientFactory restTemplateTransportClientFactory =
                new RestTemplateTransportClientFactory();
        EurekaEndpoint eurekaEndpoint = new DefaultEndpoint(serverAddressSupplier.get());
        EurekaHttpClient eurekaHttpClient = restTemplateTransportClientFactory.newClient(eurekaEndpoint);
        return new EurekaNamingService(eurekaHttpClient);
    }
}
