package com.alibaba.nacossync.extension.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.ConsulServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.ecwid.consul.transport.HttpResponse;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.model.HealthService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author paderlol
 * @date: 2019-01-12 20:58
 * @see ConsulSyncToNacosServiceImpl
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsulSyncToNacosServiceImplTest {

    public static final String TEST_SOURCE_CLUSTER_ID = "test-source-cluster-id";
    public static final String TEST_DEST_CLUSTER_ID = "test-dest-cluster-id";
    public static final String TEST_TASK_ID = "test-task-id";
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

    @InjectMocks
    @Spy
    private ConsulSyncToNacosServiceImpl consulSyncToNacosService;

    @Test
    public void testConsulSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockSync(taskDO);
        // TODO Test the core logic in the future
        Assert.assertTrue(consulSyncToNacosService.sync(taskDO,null));
    }

    @Test
    public void testConsulDeleteSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockDelete(taskDO);
        // TODO Test the core logic in the future
        Assert.assertTrue(consulSyncToNacosService.delete(taskDO));
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
        Instance instance = mock(Instance.class);
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, TEST_SOURCE_CLUSTER_ID);
        HealthService healthServiceUp = buildHealthService(TEST_INSTANCE_ADDRESS, 8080, Maps.newHashMap());
        HealthService healthServiceDown = buildHealthService(TEST_INSTANCE_ADDRESS, 8081, metadata);
        List<HealthService> healthServiceList = Lists.newArrayList(healthServiceUp, healthServiceDown);
        HttpResponse rawResponse = new HttpResponse(200, null, null, 1000L, true, 100L);
        Response<List<HealthService>> response = new Response<>(healthServiceList, rawResponse);
        when(taskDO.getTaskId()).thenReturn(TEST_TASK_ID);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        doReturn(destNamingService).when(nacosServerHolder).get(anyString());
        doReturn(consulClient).when(consulServerHolder).get(anyString());
        doReturn(response).when(consulClient).getHealthServices(anyString(), anyBoolean(), any());
        List<Instance> allInstances = Lists.newArrayList(instance);
        doReturn(allInstances).when(destNamingService).getAllInstances(anyString());
        doReturn(ClusterTypeEnum.EUREKA).when(skyWalkerCacheServices).getClusterType(any());

    }

    private HealthService buildHealthService(String address, int port, Map<String, String> metadata) {
        HealthService healthService = new HealthService();
        HealthService.Node node = new HealthService.Node();
        node.setMeta(metadata);
        HealthService.Service service = new HealthService.Service();
        service.setAddress(address);
        service.setPort(port);
        healthService.setNode(node);
        healthService.setService(service);

        return healthService;
    }

    public void mockDelete(TaskDO taskDO) throws Exception {
        Instance instance = mock(Instance.class);
        when(taskDO.getTaskId()).thenReturn(TEST_TASK_ID);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        doReturn(destNamingService).when(nacosServerHolder).get(anyString());
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, TEST_SOURCE_CLUSTER_ID);
        List<Instance> allInstances = Lists.newArrayList(instance);
        doReturn(allInstances).when(destNamingService).getAllInstances(anyString());
        doReturn(metadata).when(instance).getMetadata();
    }

}
