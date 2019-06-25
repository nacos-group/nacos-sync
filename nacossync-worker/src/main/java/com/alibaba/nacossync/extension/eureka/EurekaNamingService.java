package com.alibaba.nacossync.extension.eureka;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import org.springframework.http.HttpStatus;

import java.util.List;
import java.util.Objects;

/**
 * @author: Liu Jun Jie
 * @create: 2019-06-24 17:01
 **/
public class EurekaNamingService {
    private EurekaHttpClient eurekaHttpClient;
    private EurekaBeatReactor beatReactor;


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