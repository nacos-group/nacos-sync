package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author paderlol
 * @date: 2019-01-12 20:58
 * @see NacosSyncToNacosServiceImpl
 */
@RunWith(MockitoJUnitRunner.class)
public class NacosSyncToNacosServiceImplTest {
    public static final String TEST_SOURCE_CLUSTER_ID = "test-source-cluster-id";
    public static final String TEST_DEST_CLUSTER_ID = "test-dest-cluster-id";
    public static final String TEST_TASK_ID = "test-task-id";
    public static final String TEST_SERVICE_NAME = "test-service";
    public static final String TEST_VERSION = "1";
    public static final String TEST_INSTANCE_ADDRESS = "127.0.0.1";
    @Mock
    private NacosServerHolder nacosServerHolder;
    @Mock
    private NamingService sourceNamingService;

    @Mock
    private NamingService destNamingService;
    @Mock
    private ClusterAccessService clusterAccessService;
    @Mock
    private SkyWalkerCacheServices skyWalkerCacheServices;
    @Mock
    private MetricsManager metricsManager;

    @InjectMocks
    @Spy
    private NacosSyncToNacosServiceImpl nacosSyncToNacosService;

    @Test
    @SuppressWarnings("unchecked")
    public void testZookeeperSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockSync(taskDO);
        Assert.assertTrue(nacosSyncToNacosService.sync(taskDO,null));

        // The native source instance is registered; the one already tagged as synced is not synced back.
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
        Assert.assertEquals(ClusterTypeEnum.NACOS.getCode(), metadata.get(SkyWalkerConstants.SYNC_SOURCE_KEY));
        Assert.assertEquals(TEST_SOURCE_CLUSTER_ID + "@@" + TEST_VERSION,
                metadata.get(SkyWalkerConstants.SYNC_INSTANCE_TAG));

        // Only the destination instance synced from this source and gone from it is deregistered.
        ArgumentCaptor<List<Instance>> deregistered = ArgumentCaptor.forClass(List.class);
        verify(destNamingService).batchDeregisterInstance(eq(TEST_SERVICE_NAME), eq(Constants.DEFAULT_GROUP),
                deregistered.capture());
        Assert.assertEquals(1, deregistered.getValue().size());
        Assert.assertEquals(8082, deregistered.getValue().get(0).getPort());

        verify(sourceNamingService).subscribe(eq(TEST_SERVICE_NAME), eq(Constants.DEFAULT_GROUP),
                any(EventListener.class));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        // Sync first so that delete has a subscription to cancel.
        mockSync(taskDO);
        Assert.assertTrue(nacosSyncToNacosService.sync(taskDO, null));
        ArgumentCaptor<EventListener> listener = ArgumentCaptor.forClass(EventListener.class);
        verify(sourceNamingService).subscribe(eq(TEST_SERVICE_NAME), eq(Constants.DEFAULT_GROUP), listener.capture());

        mockDelete();
        Assert.assertTrue(nacosSyncToNacosService.delete(taskDO));

        verify(sourceNamingService).unsubscribe(TEST_SERVICE_NAME, Constants.DEFAULT_GROUP, listener.getValue());
        // The second batch deregistration comes from delete: every source instance that sync would register.
        ArgumentCaptor<List<Instance>> deregistered = ArgumentCaptor.forClass(List.class);
        verify(destNamingService, times(2)).batchDeregisterInstance(eq(TEST_SERVICE_NAME),
                eq(Constants.DEFAULT_GROUP), deregistered.capture());
        List<Instance> removed = deregistered.getAllValues().get(1);
        Assert.assertEquals(1, removed.size());
        Assert.assertEquals(8080, removed.get(0).getPort());
    }

    @Test(expected = Exception.class)
    public void testNacosSyncToNacosWithException() throws Exception {
        Assert.assertFalse(nacosSyncToNacosService.sync(null, null));
    }
    @Test(expected = Exception.class)
    public void testNacosDeleteToNacosWithException() throws Exception {
        Assert.assertFalse(nacosSyncToNacosService.delete(null));
    }

    public void mockSync(TaskDO taskDO) throws Exception {
        when(taskDO.getTaskId()).thenReturn(TEST_TASK_ID);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        when(taskDO.getServiceName()).thenReturn(TEST_SERVICE_NAME);
        when(taskDO.getVersion()).thenReturn(TEST_VERSION);
        doReturn(destNamingService).when(nacosServerHolder).get(TEST_DEST_CLUSTER_ID);
        doReturn(sourceNamingService).when(nacosServerHolder).get(TEST_SOURCE_CLUSTER_ID);
        when(clusterAccessService.findClusterLevel(TEST_SOURCE_CLUSTER_ID)).thenReturn(0);
        doReturn(ClusterTypeEnum.NACOS).when(skyWalkerCacheServices).getClusterType(TEST_SOURCE_CLUSTER_ID);
        doReturn(sourceInstances()).when(sourceNamingService)
                .getAllInstances(eq(TEST_SERVICE_NAME), eq(Constants.DEFAULT_GROUP), anyList(), eq(true));

        Map<String, String> syncedMetadata = Maps.newHashMap();
        syncedMetadata.put(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, TEST_SOURCE_CLUSTER_ID);
        List<Instance> destInstances = Lists.newArrayList(buildInstance(8082, syncedMetadata),
                buildInstance(8083, Maps.newHashMap()));
        doReturn(destInstances).when(destNamingService)
                .getAllInstances(eq(TEST_SERVICE_NAME), eq(Constants.DEFAULT_GROUP), anyList(), eq(true));
    }

    public void mockDelete() throws Exception {
        doReturn(sourceInstances()).when(sourceNamingService)
                .getAllInstances(eq(TEST_SERVICE_NAME), eq(Constants.DEFAULT_GROUP), anyList(), eq(false));
    }

    private List<Instance> sourceInstances() {
        Map<String, String> syncedMetadata = Maps.newHashMap();
        syncedMetadata.put(SkyWalkerConstants.SYNC_INSTANCE_TAG, "other-cluster@@1");
        return Lists.newArrayList(buildInstance(8080, Maps.newHashMap()), buildInstance(8081, syncedMetadata));
    }

    private Instance buildInstance(int port, Map<String, String> metadata) {
        Instance instance = new Instance();
        instance.setIp(TEST_INSTANCE_ADDRESS);
        instance.setPort(port);
        instance.setServiceName(TEST_SERVICE_NAME);
        instance.setMetadata(metadata);
        return instance;
    }

}
