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

import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.netflix.discovery.shared.resolver.DefaultEndpoint;
import com.netflix.discovery.shared.resolver.EurekaEndpoint;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.netflix.eureka.http.RestTemplateTransportClientFactory;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;


/**
 * @author paderlol
 * @date: 2018-12-31 16:26
 */
@Service
@Slf4j
public class EurekaServerHolder extends AbstractServerHolderImpl<EurekaNamingService> {
    
    private static final String HTTP_PREFIX = "http://";
    
    private static final String HTTPS_PREFIX = "https://";
    
    @Override
    EurekaNamingService createServer(String clusterId, Supplier<String> serverAddressSupplier) throws Exception {
        RestTemplateTransportClientFactory restTemplateTransportClientFactory = new RestTemplateTransportClientFactory();
        EurekaEndpoint eurekaEndpoint = new DefaultEndpoint(addHttpPrefix(serverAddressSupplier.get()));
        EurekaHttpClient eurekaHttpClient = restTemplateTransportClientFactory.newClient(eurekaEndpoint);
        return new EurekaNamingService(eurekaHttpClient);
    }
    
    public String addHttpPrefix(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        if (!input.startsWith(HTTP_PREFIX) && !input.startsWith(HTTPS_PREFIX)) {
            input = HTTP_PREFIX + input;
        }
        
        return input;
    }
}
