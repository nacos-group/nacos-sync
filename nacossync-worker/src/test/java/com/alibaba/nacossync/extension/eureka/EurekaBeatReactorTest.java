package com.alibaba.nacossync.extension.eureka;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EurekaBeatReactorTest {

    private static final String APP = "SPRING-CLOUD-NACOS-CONSUMER";

    private static final String ID = "192.168.31.92:spring-cloud-nacos-consumer:18035";

    private final EurekaHttpClient eurekaHttpClient = mock(EurekaHttpClient.class);

    private final EurekaBeatReactor beatReactor = new EurekaBeatReactor(eurekaHttpClient);

    private final InstanceInfo instance = InstanceInfo.Builder.newBuilder().setAppName(APP).setInstanceId(ID)
            .setHostName("192.168.31.92").build();

    @Test
    void instanceUnknownToEurekaIsRegisteredAgain() {
        // After a Eureka restart the registry is empty and every heartbeat gets a 404, like DiscoveryClient handles it.
        EurekaHttpResponse<InstanceInfo> notFound = notFound();
        when(eurekaHttpClient.sendHeartBeat(eq(APP), eq(ID), any(), any())).thenReturn(notFound);
        when(eurekaHttpClient.register(instance)).thenReturn(EurekaHttpResponse.status(204));
        beatReactor.addInstance(ID, instance);

        beatReactor.beat(instance);

        verify(eurekaHttpClient).register(instance);
    }

    @Test
    void removedInstanceIsNotRegisteredAgain() {
        // A heartbeat still in flight when the instance is deregistered must not bring it back.
        EurekaHttpResponse<InstanceInfo> notFound = notFound();
        when(eurekaHttpClient.sendHeartBeat(eq(APP), eq(ID), any(), any())).thenReturn(notFound);

        beatReactor.beat(instance);

        verify(eurekaHttpClient, never()).register(any());
    }

    @SuppressWarnings("unchecked")
    private static EurekaHttpResponse<InstanceInfo> notFound() {
        EurekaHttpResponse<InstanceInfo> response = mock(EurekaHttpResponse.class);
        when(response.getStatusCode()).thenReturn(404);
        return response;
    }
}
