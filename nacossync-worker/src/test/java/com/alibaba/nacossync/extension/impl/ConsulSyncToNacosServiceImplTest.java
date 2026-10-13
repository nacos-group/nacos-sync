package com.alibaba.nacossync.extension.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.ConsulServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.ecwid.consul.transport.HttpResponse;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.model.HealthService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author paderlol
 * @date: 2019-01-12 20:58
 * @see ConsulSyncToNacosServiceImpl
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsulSyncToNacosServiceImplTest {

    public static final String TEST_SOURCE_CLUSTER_ID = "test-source-cluster-id";
    public static final String TEST_DEST_CLUSTER_ID = "test-dest-cluster-id";
    public static final String TEST_SERVICE_NAME = "test-service";
    public static final String TEST_INSTANCE_ADDRESS = "127.0.0.1";
    @Mock
    private NacosServerHolder nacosServerHolder;
    @Mock
    private ConsulClient consulClient;

    @Mock
    private ConsulServerHolder consulServerHolder;
    @Mock
    private NamingService destNamingService;
    @Mock
    private SkyWalkerCacheServices skyWalkerCacheServices;
    @Mock
    private SpecialSyncEventBus specialSyncEventBus;
    @Mock
    private MetricsManager metricsManager;

    @InjectMocks
    @Spy
    private ConsulSyncToNacosServiceImpl consulSyncToNacosService;

    @Test
    @SuppressWarnings("unchecked")
    public void testConsulSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockSync(taskDO);
        Assert.assertTrue(consulSyncToNacosService.sync(taskDO,null));

        // Only the native Consul instance is registered; the one tagged with a source cluster came from a sync.
        ArgumentCaptor<List<Instance>> registered = ArgumentCaptor.forClass(List.class);
        verify(destNamingService).batchRegisterInstance(eq(TEST_SERVICE_NAME), eq(Constants.DEFAULT_GROUP),
                registered.capture());
        Assert.assertEquals(1, registered.getValue().size());
        Instance instance = registered.getValue().get(0);
        Assert.assertEquals(TEST_INSTANCE_ADDRESS, instance.getIp());
        Assert.assertEquals(8080, instance.getPort());
        Map<String, String> metadata = instance.getMetadata();
        Assert.assertEquals(TEST_SOURCE_CLUSTER_ID, metadata.get(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY));
        Assert.assertEquals(TEST_DEST_CLUSTER_ID, metadata.get(SkyWalkerConstants.DEST_CLUSTER_ID_KEY));
        Assert.assertEquals(ClusterTypeEnum.CONSUL.getCode(), metadata.get(SkyWalkerConstants.SYNC_SOURCE_KEY));

        // The previously synced instance that is gone from Consul is removed, the live one is kept.
        verify(destNamingService).deregisterInstance(TEST_SERVICE_NAME, Constants.DEFAULT_GROUP,
                TEST_INSTANCE_ADDRESS, 8082);
        verify(destNamingService, never()).deregisterInstance(anyString(), anyString(), anyString(), eq(8080));
        verify(specialSyncEventBus).subscribe(eq(taskDO), any());
    }

    @Test
    public void testConsulDeleteSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockDelete(taskDO);
        Assert.assertTrue(consulSyncToNacosService.delete(taskDO));

        verify(specialSyncEventBus).unsubscribe(taskDO);
        verify(destNamingService).deregisterInstance(TEST_SERVICE_NAME, Constants.DEFAULT_GROUP,
                TEST_INSTANCE_ADDRESS, 8080);
        // Instances not synced from this source cluster are left alone.
        verify(destNamingService, never()).deregisterInstance(anyString(), anyString(), anyString(), eq(8081));
    }

    @Test(expected = Exception.class)
    public void testConsulSyncToNacosWithException() throws Exception {
        Assert.assertFalse(consulSyncToNacosService.sync(null,null));
    }

    @Test(expected = Exception.class)
    public void testConsulDeleteToNacosWithException() throws Exception {
        Assert.assertFalse(consulSyncToNacosService.delete(null));
    }

    public void mockSync(TaskDO taskDO) throws Exception {
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, TEST_SOURCE_CLUSTER_ID);
        HealthService healthServiceUp = buildHealthService(TEST_INSTANCE_ADDRESS, 8080, Maps.newHashMap());
        HealthService healthServiceDown = buildHealthService(TEST_INSTANCE_ADDRESS, 8081, metadata);
        List<HealthService> healthServiceList = Lists.newArrayList(healthServiceUp, healthServiceDown);
        HttpResponse rawResponse = new HttpResponse(200, null, null, 1000L, true, 100L);
        Response<List<HealthService>> response = new Response<>(healthServiceList, rawResponse);
        when(taskDO.getServiceName()).thenReturn(TEST_SERVICE_NAME);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        doReturn(destNamingService).when(nacosServerHolder).get(TEST_DEST_CLUSTER_ID);
        doReturn(consulClient).when(consulServerHolder).get(TEST_SOURCE_CLUSTER_ID);
        doReturn(response).when(consulClient).getHealthServices(TEST_SERVICE_NAME, true, QueryParams.DEFAULT);
        List<Instance> allInstances = Lists.newArrayList(buildInstance(8080, metadata),
                buildInstance(8082, metadata));
        doReturn(allInstances).when(destNamingService).getAllInstances(TEST_SERVICE_NAME);
        doReturn(ClusterTypeEnum.CONSUL).when(skyWalkerCacheServices).getClusterType(TEST_SOURCE_CLUSTER_ID);

    }

    private HealthService buildHealthService(String address, int port, Map<String, String> metadata) {
        HealthService healthService = new HealthService();
        HealthService.Service service = new HealthService.Service();
        service.setAddress(address);
        service.setPort(port);
        // Consul carries instance metadata as "key=value" tags
        service.setTags(metadata.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toList());
        healthService.setService(service);

        return healthService;
    }

    private Instance buildInstance(int port, Map<String, String> metadata) {
        Instance instance = new Instance();
        instance.setIp(TEST_INSTANCE_ADDRESS);
        instance.setPort(port);
        instance.setMetadata(metadata);
        return instance;
    }

    public void mockDelete(TaskDO taskDO) throws Exception {
        when(taskDO.getServiceName()).thenReturn(TEST_SERVICE_NAME);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        doReturn(destNamingService).when(nacosServerHolder).get(TEST_DEST_CLUSTER_ID);
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, TEST_SOURCE_CLUSTER_ID);
        List<Instance> allInstances = Lists.newArrayList(buildInstance(8080, metadata),
                buildInstance(8081, Maps.newHashMap()));
        doReturn(allInstances).when(destNamingService).getAllInstances(TEST_SERVICE_NAME, Constants.DEFAULT_GROUP);
    }

}
