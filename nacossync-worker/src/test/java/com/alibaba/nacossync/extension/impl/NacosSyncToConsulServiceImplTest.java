package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.holder.ConsulServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.model.HealthService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NacosSyncToConsulServiceImplTest {

    private static final String SOURCE_CLUSTER_ID = "nacos-cluster";

    private static final String DEST_CLUSTER_ID = "consul-cluster";

    private static final String SERVICE_NAME = "demo-service";

    private static final String IP = "127.0.0.1";

    private final NamingService sourceNamingService = mock(NamingService.class);

    private final ConsulClient consulClient = mock(ConsulClient.class);

    private NacosSyncToConsulServiceImpl nacosSyncToConsulService;

    private TaskDO taskDO;

    @BeforeEach
    void setUp() {
        NacosServerHolder nacosServerHolder = mock(NacosServerHolder.class);
        ConsulServerHolder consulServerHolder = mock(ConsulServerHolder.class);
        SkyWalkerCacheServices skyWalkerCacheServices = mock(SkyWalkerCacheServices.class);
        when(nacosServerHolder.get(SOURCE_CLUSTER_ID)).thenReturn(sourceNamingService);
        when(consulServerHolder.get(DEST_CLUSTER_ID)).thenReturn(consulClient);
        when(skyWalkerCacheServices.getClusterType(SOURCE_CLUSTER_ID)).thenReturn(ClusterTypeEnum.NACOS);

        nacosSyncToConsulService = new NacosSyncToConsulServiceImpl(skyWalkerCacheServices, consulServerHolder);
        ReflectionTestUtils.setField(nacosSyncToConsulService, "nacosServerHolder", nacosServerHolder);
        ReflectionTestUtils.setField(nacosSyncToConsulService, "metricsManager", mock(MetricsManager.class));

        taskDO = new TaskDO();
        taskDO.setTaskId("task-id");
        taskDO.setSourceClusterId(SOURCE_CLUSTER_ID);
        taskDO.setDestClusterId(DEST_CLUSTER_ID);
        taskDO.setServiceName(SERVICE_NAME);
    }

    @Test
    void onlyInstanceThatWentOfflineIsDeregisteredFromConsul() throws Exception {
        givenSourceInstances(List.of(nacosInstance(8080), nacosInstance(8081)), List.of(nacosInstance(8080)));
        when(consulClient.getHealthServices(SERVICE_NAME, true, QueryParams.DEFAULT)).thenReturn(
                new Response<>(List.of(syncedConsulService(8080), syncedConsulService(8081)), 0L, true, 0L));

        assertTrue(nacosSyncToConsulService.sync(taskDO, 0));
        assertTrue(nacosSyncToConsulService.sync(taskDO, 0));

        verify(consulClient).agentServiceDeregister(serviceId(8081));
        verify(consulClient, never()).agentServiceDeregister(serviceId(8080));
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

    private Instance nacosInstance(int port) {
        Instance instance = new Instance();
        instance.setInstanceId(serviceId(port));
        instance.setIp(IP);
        instance.setPort(port);
        instance.setMetadata(new HashMap<>());
        return instance;
    }

    private HealthService syncedConsulService(int port) {
        HealthService.Service service = new HealthService.Service();
        service.setId(serviceId(port));
        service.setService(SERVICE_NAME);
        service.setAddress(IP);
        service.setPort(port);
        service.setTags(List.of(String.join("=", SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, SOURCE_CLUSTER_ID)));
        HealthService healthService = new HealthService();
        healthService.setService(service);
        return healthService;
    }

    private String serviceId(int port) {
        return SERVICE_NAME + "-" + port;
    }
}
