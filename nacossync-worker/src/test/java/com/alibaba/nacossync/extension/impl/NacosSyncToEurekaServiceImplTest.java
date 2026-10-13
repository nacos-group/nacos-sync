package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.netflix.appinfo.InstanceInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Nacos health, enabled state and metadata must be reflected in Eureka (issue #394).
 */
class NacosSyncToEurekaServiceImplTest {

    private static final String SOURCE_CLUSTER_ID = "nacos-cluster";

    private static final String DEST_CLUSTER_ID = "eureka-cluster";

    private static final String SERVICE_NAME = "demo-service";

    private static final String IP = "127.0.0.1";

    private final NamingService sourceNamingService = mock(NamingService.class);

    private final EurekaNamingService eurekaNamingService = mock(EurekaNamingService.class);

    private NacosSyncToEurekaServiceImpl nacosSyncToEurekaService;

    private TaskDO taskDO;

    @BeforeEach
    void setUp() {
        NacosServerHolder nacosServerHolder = mock(NacosServerHolder.class);
        EurekaServerHolder eurekaServerHolder = mock(EurekaServerHolder.class);
        SkyWalkerCacheServices skyWalkerCacheServices = mock(SkyWalkerCacheServices.class);
        when(nacosServerHolder.get(SOURCE_CLUSTER_ID)).thenReturn(sourceNamingService);
        when(eurekaServerHolder.get(DEST_CLUSTER_ID)).thenReturn(eurekaNamingService);
        when(skyWalkerCacheServices.getClusterType(SOURCE_CLUSTER_ID)).thenReturn(ClusterTypeEnum.NACOS);

        nacosSyncToEurekaService = new NacosSyncToEurekaServiceImpl(eurekaServerHolder, skyWalkerCacheServices);
        ReflectionTestUtils.setField(nacosSyncToEurekaService, "nacosServerHolder", nacosServerHolder);
        ReflectionTestUtils.setField(nacosSyncToEurekaService, "metricsManager", mock(MetricsManager.class));

        taskDO = new TaskDO();
        taskDO.setTaskId("task-id");
        taskDO.setSourceClusterId(SOURCE_CLUSTER_ID);
        taskDO.setDestClusterId(DEST_CLUSTER_ID);
        taskDO.setServiceName(SERVICE_NAME);
    }

    @Test
    void unhealthyOrDisabledInstancesAreNotRegisteredInEureka() throws Exception {
        Instance unhealthy = nacosInstance(8081, Map.of());
        unhealthy.setHealthy(false);
        Instance disabled = nacosInstance(8082, Map.of());
        disabled.setEnabled(false);
        givenSourceInstances(List.of(nacosInstance(8080, Map.of()), unhealthy, disabled));

        assertTrue(nacosSyncToEurekaService.sync(taskDO, 0));

        ArgumentCaptor<InstanceInfo> registered = ArgumentCaptor.forClass(InstanceInfo.class);
        verify(eurekaNamingService).registerInstance(registered.capture());
        assertEquals(8080, registered.getValue().getPort());
    }

    @Test
    void instanceThatTurnsUnhealthyIsRemovedFromEureka() throws Exception {
        Instance turnsUnhealthy = nacosInstance(8081, Map.of());
        turnsUnhealthy.setHealthy(false);
        givenSourceInstances(List.of(nacosInstance(8080, Map.of()), nacosInstance(8081, Map.of())),
                List.of(nacosInstance(8080, Map.of()), turnsUnhealthy));
        InstanceInfo synced8080 = syncedEurekaInstance(8080);
        InstanceInfo synced8081 = syncedEurekaInstance(8081);
        when(eurekaNamingService.getApplications(SERVICE_NAME)).thenReturn(List.of(synced8080, synced8081));

        assertTrue(nacosSyncToEurekaService.sync(taskDO, 0));
        assertTrue(nacosSyncToEurekaService.sync(taskDO, 0));

        verify(eurekaNamingService).deregisterInstance(synced8081);
        verify(eurekaNamingService, never()).deregisterInstance(synced8080);
    }

    /**
     * Guards the current re-register-on-every-sync behaviour: an existing instance whose metadata changed must be
     * registered again with the new metadata.
     */
    @Test
    void metadataChangeIsPropagatedToEureka() throws Exception {
        givenSourceInstances(List.of(nacosInstance(8080, Map.of("version", "v1"))),
                List.of(nacosInstance(8080, Map.of("version", "v2"))));

        assertTrue(nacosSyncToEurekaService.sync(taskDO, 0));
        assertTrue(nacosSyncToEurekaService.sync(taskDO, 0));

        ArgumentCaptor<InstanceInfo> registered = ArgumentCaptor.forClass(InstanceInfo.class);
        verify(eurekaNamingService, atLeastOnce()).registerInstance(registered.capture());
        assertEquals("v2", registered.getValue().getMetadata().get("version"));
    }

    /**
     * Each sync reads the next snapshot of source instances.
     */
    @SafeVarargs
    private void givenSourceInstances(List<Instance>... snapshots) throws Exception {
        OngoingStubbing<List<Instance>> stubbing = when(
                sourceNamingService.getAllInstances(eq(SERVICE_NAME), eq(Constants.DEFAULT_GROUP), anyList(),
                        eq(true)));
        for (List<Instance> snapshot : snapshots) {
            stubbing = stubbing.thenReturn(new ArrayList<>(snapshot));
        }
    }

    private Instance nacosInstance(int port, Map<String, String> metadata) {
        Instance instance = new Instance();
        instance.setIp(IP);
        instance.setPort(port);
        instance.setMetadata(new HashMap<>(metadata));
        return instance;
    }

    private InstanceInfo syncedEurekaInstance(int port) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, SOURCE_CLUSTER_ID);
        // InstanceInfo.equals compares ids only, so each synced instance needs its own
        return InstanceInfo.Builder.newBuilder().setInstanceId(IP + ":" + SERVICE_NAME + ":" + port).setIPAddr(IP)
                .setPort(port).setAppName(SERVICE_NAME).setMetadata(metadata).build();
    }
}
