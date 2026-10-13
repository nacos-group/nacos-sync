package com.alibaba.nacossync.extension.eureka;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EurekaNamingServiceTest {

    private static final String APP = "SPRING-CLOUD-NACOS-CONSUMER";

    private static final String ID = "192.168.31.92:spring-cloud-nacos-consumer:18035";

    private final EurekaHttpClient eurekaHttpClient = mock(EurekaHttpClient.class);

    private final EurekaBeatReactor beatReactor = mock(EurekaBeatReactor.class);

    private final EurekaNamingService namingService = new EurekaNamingService(eurekaHttpClient, beatReactor);

    private final InstanceInfo instance = InstanceInfo.Builder.newBuilder().setAppName(APP).setInstanceId(ID)
            .setHostName("192.168.31.92").build();

    @Test
    void heartbeatStopsBeforeTheInstanceIsCancelled() {
        // A heartbeat sent after the cancel makes a peer that replicates it re-register the instance.
        when(eurekaHttpClient.cancel(APP, ID)).thenReturn(EurekaHttpResponse.status(200));

        namingService.deregisterInstance(instance);

        InOrder order = inOrder(beatReactor, eurekaHttpClient);
        order.verify(beatReactor).removeInstance(ID);
        order.verify(eurekaHttpClient).cancel(APP, ID);
    }

    @Test
    void heartbeatStopsEvenWhenTheCancelFails() {
        // Otherwise nacos-sync keeps renewing an instance it meant to remove, and it never expires.
        when(eurekaHttpClient.cancel(APP, ID)).thenReturn(EurekaHttpResponse.status(404));

        namingService.deregisterInstance(instance);

        verify(beatReactor).removeInstance(ID);
    }
}
