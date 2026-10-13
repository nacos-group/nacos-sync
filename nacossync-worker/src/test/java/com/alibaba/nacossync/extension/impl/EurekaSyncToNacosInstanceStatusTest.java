package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Eureka instance status must be reflected in Nacos: only UP instances are routable (issue #256).
 */
class EurekaSyncToNacosInstanceStatusTest {

    private static final String SOURCE_CLUSTER_ID = "eureka-cluster";

    private static final String DEST_CLUSTER_ID = "nacos-cluster";

    private static final String SERVICE_NAME = "SPRING-CLOUD-EUREKA-CLIENT";

    private static final String IP = "127.0.0.1";

    private final EurekaNamingService eurekaNamingService = mock(EurekaNamingService.class);

    private final NamingService destNamingService = mock(NamingService.class);

    private EurekaSyncToNacosServiceImpl eurekaSyncToNacosService;

    private TaskDO taskDO;

    @BeforeEach
    void setUp() {
        EurekaServerHolder eurekaServerHolder = mock(EurekaServerHolder.class);
        NacosServerHolder nacosServerHolder = mock(NacosServerHolder.class);
        SkyWalkerCacheServices skyWalkerCacheServices = mock(SkyWalkerCacheServices.class);
        when(eurekaServerHolder.get(SOURCE_CLUSTER_ID)).thenReturn(eurekaNamingService);
        when(nacosServerHolder.get(DEST_CLUSTER_ID)).thenReturn(destNamingService);
        when(skyWalkerCacheServices.getClusterType(SOURCE_CLUSTER_ID)).thenReturn(ClusterTypeEnum.EUREKA);

        eurekaSyncToNacosService = new EurekaSyncToNacosServiceImpl(eurekaServerHolder, skyWalkerCacheServices,
                nacosServerHolder, mock(SpecialSyncEventBus.class), mock(MetricsManager.class));

        taskDO = new TaskDO();
        taskDO.setTaskId("task-id");
        taskDO.setSourceClusterId(SOURCE_CLUSTER_ID);
        taskDO.setDestClusterId(DEST_CLUSTER_ID);
        taskDO.setServiceName(SERVICE_NAME);
    }

    @Test
    @SuppressWarnings("unchecked")
    void onlyUpInstancesAreRegisteredInNacos() throws Exception {
        when(eurekaNamingService.getApplications(SERVICE_NAME)).thenReturn(
                List.of(eurekaInstance(8080, InstanceStatus.UP), eurekaInstance(8081, InstanceStatus.DOWN),
                        eurekaInstance(8082, InstanceStatus.OUT_OF_SERVICE),
                        eurekaInstance(8083, InstanceStatus.STARTING)));

        assertTrue(eurekaSyncToNacosService.sync(taskDO, 0));

        ArgumentCaptor<List> registered = ArgumentCaptor.forClass(List.class);
        verify(destNamingService).batchRegisterInstance(eq(SERVICE_NAME), eq(Constants.DEFAULT_GROUP),
                registered.capture());
        List<Instance> instances = registered.getValue();
        assertEquals(List.of(8080), instances.stream().map(Instance::getPort).toList());
    }

    @Test
    void syncedInstancesAreRemovedWhenNoEurekaInstanceIsUp() throws Exception {
        Instance synced = syncedNacosInstance(8081);
        when(eurekaNamingService.getApplications(SERVICE_NAME)).thenReturn(
                List.of(eurekaInstance(8081, InstanceStatus.DOWN)));
        when(destNamingService.getAllInstances(SERVICE_NAME, Constants.DEFAULT_GROUP)).thenReturn(List.of(synced));

        assertTrue(eurekaSyncToNacosService.sync(taskDO, 0));

        verify(destNamingService).deregisterInstance(SERVICE_NAME, Constants.DEFAULT_GROUP, synced);
        verify(destNamingService, never()).batchRegisterInstance(anyString(), anyString(), anyList());
    }

    private InstanceInfo eurekaInstance(int port, InstanceStatus status) {
        return InstanceInfo.Builder.newBuilder().setIPAddr(IP).setPort(port).setStatus(status)
                .setAppName(SERVICE_NAME).build();
    }

    private Instance syncedNacosInstance(int port) {
        Instance instance = new Instance();
        instance.setIp(IP);
        instance.setPort(port);
        Map<String, String> metadata = new HashMap<>();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, SOURCE_CLUSTER_ID);
        instance.setMetadata(metadata);
        return instance;
    }
}
