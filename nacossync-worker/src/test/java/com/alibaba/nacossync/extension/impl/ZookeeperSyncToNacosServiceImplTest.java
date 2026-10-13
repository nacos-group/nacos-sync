package com.alibaba.nacossync.extension.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.DubboConstants;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author paderlol
 * @date: 2019-01-12 14:39
 * @see ZookeeperSyncToNacosServiceImpl
 */
@RunWith(MockitoJUnitRunner.class)
public class ZookeeperSyncToNacosServiceImplTest {

    private static final String TEST_SERVICE_NAME = "org.apache.dubbo.demo.DemoService";
    private static final String TEST_PROVIDER =
        "hessian%3A%2F%2F172.16.0.10%3A20880%2Forg.apache.dubbo.demo.DemoService%3Fanyhost%3Dtrue%26application%3Ddemo-provider%26dubbo%3D2.0.2%26generic%3Dfalse%26group%3DtestGroup%26interface%3Dorg.apache.dubbo.demo.DemoService%26methods%3DsayHello%26pid%3D5956%26revision%3D1.0.0%26side%3Dprovider%26timestamp%3D1547285978821%26version%3D1.0.0%26weight%3D1";
    private static final String TEST_PROVIDER_2 = TEST_PROVIDER.replace("%3A20880%2F", "%3A20881%2F");
    private static final String TEST_NACOS_SERVICE_NAME = "providers:org.apache.dubbo.demo.DemoService:1.0.0:testGroup";
    private static final String TEST_INSTANCE_ADDRESS = "172.16.0.10";
    public static final String TEST_SOURCE_CLUSTER_ID = "test-source-cluster-id";
    public static final String TEST_DEST_CLUSTER_ID = "test-dest-cluster-id";
    public static final String TEST_TASK_ID = "test-task-id";

    @Mock
    private ZookeeperServerHolder zookeeperServerHolder;
    @Mock
    private NacosServerHolder nacosServerHolder;
    @Mock
    private SkyWalkerCacheServices skyWalkerCacheServices;
    @Mock
    private NamingService destNamingService;
    @Mock
    private CuratorCache treeCache;
    @Mock
    private Listenable<CuratorCacheListener> listenable;
    @Mock
    private MetricsManager metricsManager;
    @InjectMocks
    @Spy
    private ZookeeperSyncToNacosServiceImpl zookeeperSyncToNacosService;

    private MockedStatic<CuratorCache> curatorCacheStatic;

    /** Children currently under /dubbo/{service}/providers in the mocked Zookeeper. */
    private List<String> providers = List.of(TEST_PROVIDER);

    @Before
    public void setUp() {
        curatorCacheStatic = mockStatic(CuratorCache.class);
    }

    @After
    public void tearDown() {
        curatorCacheStatic.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testZookeeperSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        Assert.assertTrue(mockSync(taskDO));

        ArgumentCaptor<List<Instance>> registered = ArgumentCaptor.forClass(List.class);
        verify(destNamingService).batchRegisterInstance(eq(TEST_NACOS_SERVICE_NAME), eq(Constants.DEFAULT_GROUP),
                registered.capture());
        Assert.assertEquals(1, registered.getValue().size());
        Instance instance = registered.getValue().get(0);
        Assert.assertEquals(TEST_INSTANCE_ADDRESS, instance.getIp());
        Assert.assertEquals(20880, instance.getPort());
        Map<String, String> metadata = instance.getMetadata();
        Assert.assertEquals(TEST_SOURCE_CLUSTER_ID, metadata.get(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY));
        Assert.assertEquals(TEST_DEST_CLUSTER_ID, metadata.get(SkyWalkerConstants.DEST_CLUSTER_ID_KEY));
        Assert.assertEquals(ClusterTypeEnum.ZK.getCode(), metadata.get(SkyWalkerConstants.SYNC_SOURCE_KEY));
        Assert.assertEquals("hessian", metadata.get(DubboConstants.PROTOCOL_KEY));

        verify(treeCache).start();
        verify(listenable).addListener(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testZookeeperDeleteToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockSync(taskDO);
        zookeeperSyncToNacosService.sync(taskDO,null);

        Assert.assertTrue(mockDelete(taskDO));

        // The second sync is a no-op because the task already has a running cache.
        verify(destNamingService).batchRegisterInstance(anyString(), anyString(), anyList());
        verify(treeCache).close();
        // Only the instance synced from this source cluster is deregistered.
        ArgumentCaptor<List<Instance>> deregistered = ArgumentCaptor.forClass(List.class);
        verify(destNamingService).batchDeregisterInstance(eq(TEST_NACOS_SERVICE_NAME), eq(Constants.DEFAULT_GROUP),
                deregistered.capture());
        Assert.assertEquals(1, deregistered.getValue().size());
        Assert.assertEquals(20880, deregistered.getValue().get(0).getPort());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAllProvidersOfAServiceAreRegisteredInOneBatch() throws Exception {
        // batchRegisterInstance replaces every instance this client registered for the service,
        // so one call per provider would leave only the last provider in Nacos.
        TaskDO taskDO = mock(TaskDO.class);
        providers = List.of(TEST_PROVIDER, TEST_PROVIDER_2);
        Assert.assertTrue(mockSync(taskDO));

        ArgumentCaptor<List<Instance>> registered = ArgumentCaptor.forClass(List.class);
        verify(destNamingService).batchRegisterInstance(eq(TEST_NACOS_SERVICE_NAME), eq(Constants.DEFAULT_GROUP),
                registered.capture());
        Assert.assertEquals(List.of(20880, 20881), registered.getValue().stream().map(Instance::getPort).sorted().toList());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProviderCreatedEventReRegistersEveryProvider() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        Assert.assertTrue(mockSync(taskDO));
        ArgumentCaptor<CuratorCacheListener> listener = ArgumentCaptor.forClass(CuratorCacheListener.class);
        verify(listenable).addListener(listener.capture());

        // A second provider appears in Zookeeper.
        providers = List.of(TEST_PROVIDER, TEST_PROVIDER_2);
        String path = String.format(DubboConstants.DUBBO_PATH_FORMAT, TEST_SERVICE_NAME) + "/" + TEST_PROVIDER_2;
        listener.getValue().event(CuratorCacheListener.Type.NODE_CREATED, null, new ChildData(path, null, null));

        ArgumentCaptor<List<Instance>> registered = ArgumentCaptor.forClass(List.class);
        verify(destNamingService, times(2)).batchRegisterInstance(eq(TEST_NACOS_SERVICE_NAME),
                eq(Constants.DEFAULT_GROUP), registered.capture());
        Assert.assertEquals(List.of(20880, 20881), registered.getValue().stream().map(Instance::getPort).sorted().toList());
    }

    @Test(expected = Exception.class)
    public void tesZookeeperSyncToNacosWithException() throws Exception {
        Assert.assertFalse(zookeeperSyncToNacosService.sync(null, null));
    }

    @Test(expected = Exception.class)
    public void testZookeeperDeleteToNacosWithException() throws Exception {
        Assert.assertFalse(zookeeperSyncToNacosService.delete(null));
    }

    public boolean mockSync(TaskDO taskDO) throws Exception {
        when(taskDO.getTaskId()).thenReturn(TEST_TASK_ID);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        when(taskDO.getServiceName()).thenReturn(TEST_SERVICE_NAME);
        CuratorFramework curatorFramework = mock(CuratorFramework.class);
        GetChildrenBuilder getChildrenBuilder = mock(GetChildrenBuilder.class);
        doReturn(curatorFramework).when(zookeeperServerHolder).get(TEST_SOURCE_CLUSTER_ID);
        doReturn(destNamingService).when(nacosServerHolder).get(TEST_DEST_CLUSTER_ID);
        curatorCacheStatic.when(() -> CuratorCache.build(curatorFramework, DubboConstants.DUBBO_ROOT_PATH))
                .thenReturn(treeCache);
        when(curatorFramework.getChildren()).thenReturn(getChildrenBuilder);
        when(getChildrenBuilder.forPath(String.format(DubboConstants.DUBBO_PATH_FORMAT, TEST_SERVICE_NAME)))
                .thenAnswer(invocation -> providers);
        doReturn(ClusterTypeEnum.ZK).when(skyWalkerCacheServices).getClusterType(TEST_SOURCE_CLUSTER_ID);
        when(treeCache.listenable()).thenReturn(listenable);
        return zookeeperSyncToNacosService.sync(taskDO,null);
    }

    public boolean mockDelete(TaskDO taskDO) throws Exception {
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTER_ID_KEY, TEST_SOURCE_CLUSTER_ID);
        List<Instance> allInstances = Lists.newArrayList(buildInstance(20880, metadata),
                buildInstance(20881, Maps.newHashMap()));

        doReturn(allInstances).when(destNamingService)
                .getAllInstances(eq(TEST_NACOS_SERVICE_NAME), eq(Constants.DEFAULT_GROUP), anyList(), eq(true));

        return zookeeperSyncToNacosService.delete(taskDO);
    }

    private Instance buildInstance(int port, Map<String, String> metadata) {
        Instance instance = new Instance();
        instance.setIp(TEST_INSTANCE_ADDRESS);
        instance.setPort(port);
        instance.setMetadata(metadata);
        return instance;
    }

}
