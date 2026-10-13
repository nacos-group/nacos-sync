package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.alibaba.nacossync.extension.event.SpecialSyncEvent;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.ConsulServerHolder;
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.model.HealthService;
import com.netflix.appinfo.InstanceInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Eureka and Consul sources are polled through {@link SpecialSyncEventBus}. A poll that is still running when its
 * task is deleted must not subscribe the task again, or the deleted task keeps registering instances forever.
 */
class PolledSyncDeleteRaceTest {

    private static final String SERVICE_NAME = "e2e-service";

    private final SpecialSyncEventBus specialSyncEventBus = new SpecialSyncEventBus();

    private final NamingService destNamingService = mock(NamingService.class);

    private final SkyWalkerCacheServices skyWalkerCacheServices = mock(SkyWalkerCacheServices.class);

    private final NacosServerHolder nacosServerHolder = mock(NacosServerHolder.class);

    private TaskDO taskDO;

    @BeforeEach
    void setUp() throws Exception {
        taskDO = new TaskDO();
        taskDO.setTaskId("task-id");
        taskDO.setSourceClusterId("source");
        taskDO.setDestClusterId("dest");
        taskDO.setServiceName(SERVICE_NAME);
        when(nacosServerHolder.get("dest")).thenReturn(destNamingService);
        when(destNamingService.getAllInstances(SERVICE_NAME, Constants.DEFAULT_GROUP)).thenReturn(List.of());
    }

    @Test
    void eurekaPollRunningDuringDeleteDoesNotResubscribe() {
        EurekaServerHolder eurekaServerHolder = mock(EurekaServerHolder.class);
        EurekaNamingService eurekaNamingService = mock(EurekaNamingService.class);
        when(eurekaServerHolder.get("source")).thenReturn(eurekaNamingService);
        when(eurekaNamingService.getApplications(SERVICE_NAME)).thenReturn(List.of());
        when(skyWalkerCacheServices.getClusterType("source")).thenReturn(ClusterTypeEnum.EUREKA);
        EurekaSyncToNacosServiceImpl service = new EurekaSyncToNacosServiceImpl(eurekaServerHolder,
                skyWalkerCacheServices, nacosServerHolder, specialSyncEventBus, mock(MetricsManager.class));

        assertTrue(service.sync(taskDO, 0));
        assertPollDuringDeleteLeavesNoSubscription(() -> service.delete(taskDO));
    }

    @Test
    void consulPollRunningDuringDeleteDoesNotResubscribe() {
        ConsulServerHolder consulServerHolder = mock(ConsulServerHolder.class);
        ConsulClient consulClient = mock(ConsulClient.class);
        when(consulServerHolder.get("source")).thenReturn(consulClient);
        when(consulClient.getHealthServices(SERVICE_NAME, true, QueryParams.DEFAULT))
                .thenReturn(new Response<List<HealthService>>(List.of(), 0L, true, 0L));
        ConsulSyncToNacosServiceImpl service = new ConsulSyncToNacosServiceImpl(consulServerHolder,
                skyWalkerCacheServices, nacosServerHolder, specialSyncEventBus, mock(MetricsManager.class));

        assertTrue(service.sync(taskDO, 0));
        assertPollDuringDeleteLeavesNoSubscription(() -> service.delete(taskDO));
    }

    private void assertPollDuringDeleteLeavesNoSubscription(Runnable delete) {
        assertEquals(1, specialSyncEventBus.getAllSpecialSyncEvent().size());
        SpecialSyncEvent poll = specialSyncEventBus.getAllSpecialSyncEvent().iterator().next();

        delete.run();
        // The poll that SpecialSyncEventTimer had already started finishes after the delete.
        poll.getSyncAction().accept(poll.getTaskDO());

        assertTrue(specialSyncEventBus.getAllSpecialSyncEvent().isEmpty());
    }
}
