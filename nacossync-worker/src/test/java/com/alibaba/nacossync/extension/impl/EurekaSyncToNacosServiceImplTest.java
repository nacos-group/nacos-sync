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
import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.appinfo.InstanceInfo;
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
 * @see EurekaSyncToNacosServiceImpl
 */
@RunWith(MockitoJUnitRunner.class)
public class EurekaSyncToNacosServiceImplTest {
    public static final String TEST_SOURCE_CLUSTER_ID = "test-source-cluster-id";
    public static final String TEST_DEST_CLUSTER_ID = "test-dest-cluster-id";
    public static final String TEST_SERVICE_NAME = "SPRING-CLOUD-EUREKA-CLIENT";
    public static final String TEST_INSTANCE_ADDRESS = "127.0.0.1";
    @Mock
    private NacosServerHolder nacosServerHolder;
    @Mock
    private EurekaServerHolder eurekaServerHolder;
    @Mock
    private EurekaNamingService eurekaNamingService;
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
    private EurekaSyncToNacosServiceImpl eurekaSyncToNacosService;

    @Test
    @SuppressWarnings("unchecked")
    public void testEurekaSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockSync(taskDO);
        Assert.assertTrue(eurekaSyncToNacosService.sync(taskDO,null));

        // How non-UP instances are treated is not asserted here.
        ArgumentCaptor<List<Instance>> registered = ArgumentCaptor.forClass(List.class);
        verify(destNamingService).batchRegisterInstance(eq(TEST_SERVICE_NAME), eq(Constants.DEFAULT_GROUP),
                registered.capture());
        Instance instance = registered.getValue().stream().filter(i -> i.getPort() == 8080).findFirst()
                .orElseThrow(() -> new AssertionError("UP instance was not registered"));
        Assert.assertEquals(TEST_INSTANCE_ADDRESS, instance.getIp());
        Map<String, String> metadata = instance.getMetadata();
        Assert.assertEquals(TEST_SOURCE_CLUSTER_ID, metadata.get(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY));
        Assert.assertEquals(TEST_DEST_CLUSTER_ID, metadata.get(SkyWalkerConstants.DEST_CLUSTER_ID_KEY));
        Assert.assertEquals(ClusterTypeEnum.EUREKA.getCode(), metadata.get(SkyWalkerConstants.SYNC_SOURCE_KEY));

        // The synced instance that no longer exists in Eureka is removed, the live one is kept.
        verify(destNamingService).deregisterInstance(TEST_SERVICE_NAME, Constants.DEFAULT_GROUP,
                TEST_INSTANCE_ADDRESS, 8082);
        verify(destNamingService, never()).deregisterInstance(anyString(), anyString(), anyString(), eq(8080));
        verify(specialSyncEventBus).subscribe(eq(taskDO), any());
    }



    @Test
    public void testEurekaDeleteSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockDelete(taskDO);
        Assert.assertTrue(eurekaSyncToNacosService.delete(taskDO));

        verify(specialSyncEventBus).unsubscribe(taskDO);
        // Only the instance owned by Eureka is deregistered, not the one Eureka itself got from a sync.
        ArgumentCaptor<Instance> deregistered = ArgumentCaptor.forClass(Instance.class);
        verify(destNamingService).deregisterInstance(eq(TEST_SERVICE_NAME), eq(Constants.DEFAULT_GROUP),
                deregistered.capture());
        Assert.assertEquals(TEST_INSTANCE_ADDRESS, deregistered.getValue().getIp());
        Assert.assertEquals(8080, deregistered.getValue().getPort());
    }
    @Test(expected = Exception.class)
    public void testEurekaSyncToNacosWithException() throws Exception {
        Assert.assertFalse(eurekaSyncToNacosService.sync(null, null));
    }
    @Test(expected = Exception.class)
    public void testEurekaDeleteToNacosWithException() throws Exception {
        Assert.assertFalse(eurekaSyncToNacosService.delete(null));
    }

    public void mockSync(TaskDO taskDO) throws Exception {


        InstanceInfo instanceInfoUp = InstanceInfo.Builder.newBuilder().setIPAddr(TEST_INSTANCE_ADDRESS).setPort(8080)
                .setStatus(InstanceInfo.InstanceStatus.UP).setAppName(TEST_SERVICE_NAME).build();
        InstanceInfo instanceInfoDown = InstanceInfo.Builder.newBuilder().setIPAddr(TEST_INSTANCE_ADDRESS).setPort(8081)
                .setStatus(InstanceInfo.InstanceStatus.DOWN).setAppName(TEST_SERVICE_NAME).build();
        List<InstanceInfo> allInstanceInfo = Lists.newArrayList(instanceInfoUp, instanceInfoDown);
        when(taskDO.getServiceName()).thenReturn(TEST_SERVICE_NAME);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);

        doReturn(destNamingService).when(nacosServerHolder).get(TEST_DEST_CLUSTER_ID);
        doReturn(eurekaNamingService).when(eurekaServerHolder).get(TEST_SOURCE_CLUSTER_ID);
        doReturn(allInstanceInfo).when(eurekaNamingService).getApplications(TEST_SERVICE_NAME);
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, TEST_SOURCE_CLUSTER_ID);
        List<Instance> nacosInstances = Lists.newArrayList(buildInstance(8080, metadata),
                buildInstance(8082, metadata));
        doReturn(nacosInstances).when(destNamingService)
                .getAllInstances(TEST_SERVICE_NAME, Constants.DEFAULT_GROUP);
        doReturn(ClusterTypeEnum.EUREKA).when(skyWalkerCacheServices).getClusterType(TEST_SOURCE_CLUSTER_ID);
    }

    public void mockDelete(TaskDO taskDO) throws Exception {
        when(taskDO.getServiceName()).thenReturn(TEST_SERVICE_NAME);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        doReturn(eurekaNamingService).when(eurekaServerHolder).get(TEST_SOURCE_CLUSTER_ID);
        doReturn(destNamingService).when(nacosServerHolder).get(TEST_DEST_CLUSTER_ID);
        Map<String, String> syncedMetadata = Maps.newHashMap();
        syncedMetadata.put(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, "other-cluster-id");
        List<InstanceInfo> eurekaInstances = Lists.newArrayList(
                InstanceInfo.Builder.newBuilder().setIPAddr(TEST_INSTANCE_ADDRESS).setPort(8080)
                        .setStatus(InstanceInfo.InstanceStatus.UP).setAppName(TEST_SERVICE_NAME).build(),
                InstanceInfo.Builder.newBuilder().setIPAddr(TEST_INSTANCE_ADDRESS).setPort(8081)
                        .setStatus(InstanceInfo.InstanceStatus.UP).setAppName(TEST_SERVICE_NAME)
                        .setMetadata(syncedMetadata).build());
        doReturn(eurekaInstances).when(eurekaNamingService).getApplications(TEST_SERVICE_NAME);
        doReturn(ClusterTypeEnum.EUREKA).when(skyWalkerCacheServices).getClusterType(TEST_SOURCE_CLUSTER_ID);
    }

    private Instance buildInstance(int port, Map<String, String> metadata) {
        Instance instance = new Instance();
        instance.setIp(TEST_INSTANCE_ADDRESS);
        instance.setPort(port);
        instance.setMetadata(metadata);
        return instance;
    }

}
