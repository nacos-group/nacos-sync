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
package com.alibaba.nacossync.extension.eureka;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import org.springframework.http.HttpStatus;

import java.util.List;
import java.util.Objects;

/**
 * @author liu jun jie
 * @date 2019-06-26
 */
public class EurekaNamingService {
    private final EurekaHttpClient eurekaHttpClient;
    private final EurekaBeatReactor beatReactor;


    public EurekaNamingService(EurekaHttpClient eurekaHttpClient) {
        this.eurekaHttpClient = eurekaHttpClient;
        beatReactor = new EurekaBeatReactor(eurekaHttpClient);
    }

    public void registerInstance(InstanceInfo instanceInfo) {
        EurekaHttpResponse<Void> response = eurekaHttpClient.register(instanceInfo);
        if (Objects.requireNonNull(HttpStatus.resolve(response.getStatusCode())).is2xxSuccessful()) {
            beatReactor.addInstance(instanceInfo.getId(), instanceInfo);
        }
    }

    public void deregisterInstance(InstanceInfo instanceInfo) {
        EurekaHttpResponse<Void> response = eurekaHttpClient.cancel(instanceInfo.getAppName(), instanceInfo.getId());
        if (Objects.requireNonNull(HttpStatus.resolve(response.getStatusCode())).is2xxSuccessful()) {
            beatReactor.removeInstance(instanceInfo.getId());
        }
    }

    public List<InstanceInfo> getApplications(String serviceName) {
        EurekaHttpResponse<Application> eurekaHttpResponse =
                eurekaHttpClient.getApplication(serviceName);
        if (Objects.requireNonNull(HttpStatus.resolve(eurekaHttpResponse.getStatusCode())).is2xxSuccessful()) {
            return eurekaHttpResponse.getEntity().getInstances();
        }
        return null;
    }
}