package com.alibaba.nacossync.extension.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
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
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.DubboConstants;
import java.util.List;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author paderlol
 * @date: 2019-01-12 20:16
 * @see NacosSyncToZookeeperServiceImpl
 */
@RunWith(MockitoJUnitRunner.class)
public class NacosSyncToZookeeperServiceImplTest {

    public static final String TEST_SOURCE_CLUSTER_ID = "test-source-cluster-id";
    public static final String TEST_DEST_CLUSTER_ID = "test-dest-cluster-id";
    public static final String TEST_TASK_ID = "test-task-id";
    private static final String TEST_SERVICE_NAME = "providers:org.apache.dubbo.demo.DemoService:1.0.0:";
    private static final String TEST_INTERFACE = "org.apache.dubbo.demo.DemoService";

    @Mock
    private ZookeeperServerHolder zookeeperServerHolder;
    @Mock
    private NacosServerHolder nacosServerHolder;
    @Mock
    private NamingService sourceNamingService;
    @Mock
    private SkyWalkerCacheServices skyWalkerCacheServices;
    @Mock
    private MetricsManager metricsManager;
    @Mock
    private CuratorCache pathCache;
    @InjectMocks
    @Spy
    private NacosSyncToZookeeperServiceImpl nacosSyncToZookeeperService;

    private final CuratorFramework client = mock(CuratorFramework.class, RETURNS_DEEP_STUBS);

    private MockedStatic<CuratorCache> curatorCacheStatic;

    @Before
    public void setUp() {
        curatorCacheStatic = mockStatic(CuratorCache.class);
    }

    @After
    public void tearDown() {
        curatorCacheStatic.close();
    }

    @Test
    public void testSyncWritesExistingInstancesWithoutWaitingForAChangeEvent() throws Exception {
        // nacos-client only fires a NamingEvent when its cached service info changes. A task re-added for an
        // unchanged service would never be synced if sync() waited for that event.
        TaskDO taskDO = mockTask();
        mockSourceInstances();
        when(client.checkExists().forPath(anyString())).thenReturn(null);

        Assert.assertTrue(nacosSyncToZookeeperService.sync(taskDO, null));

        verify(client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL))
                .forPath(org.mockito.ArgumentMatchers.startsWith("/dubbo/" + TEST_INTERFACE + "/providers/"));
    }

    @Test
    public void testReAddedTaskStartsWithAFreshPathCache() throws Exception {
        TaskDO taskDO = mockTask();
        mockSourceInstances();
        when(client.checkExists().forPath(anyString())).thenReturn(null);

        Assert.assertTrue(nacosSyncToZookeeperService.sync(taskDO, null));
        Assert.assertTrue(nacosSyncToZookeeperService.delete(taskDO));
        Assert.assertTrue(nacosSyncToZookeeperService.sync(taskDO, null));

        // delete() closes the task's path cache, so the re-added task must build a new one.
        curatorCacheStatic.verify(() -> CuratorCache.build(any(CuratorFramework.class), anyString()), times(2));
    }

    @Test(expected = Exception.class)
    public void testNacosSyncToZookeeperWithException() throws Exception {
        Assert.assertFalse(nacosSyncToZookeeperService.sync(null, null));
    }

    @Test(expected = Exception.class)
    public void testNacosDeleteToZookeeperWithException() throws Exception {
        Assert.assertFalse(nacosSyncToZookeeperService.delete(null));
    }

    private TaskDO mockTask() {
        TaskDO taskDO = mock(TaskDO.class);
        when(taskDO.getTaskId()).thenReturn(TEST_TASK_ID);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        when(taskDO.getServiceName()).thenReturn(TEST_SERVICE_NAME);
        doReturn(client).when(zookeeperServerHolder).get(TEST_DEST_CLUSTER_ID);
        doReturn(sourceNamingService).when(nacosServerHolder).get(TEST_SOURCE_CLUSTER_ID);
        doReturn(ClusterTypeEnum.NACOS).when(skyWalkerCacheServices).getClusterType(TEST_SOURCE_CLUSTER_ID);
        curatorCacheStatic.when(() -> CuratorCache.build(any(CuratorFramework.class), anyString())).thenReturn(pathCache);
        return taskDO;
    }

    private void mockSourceInstances() throws Exception {
        Instance instance = new Instance();
        instance.setIp("172.16.0.10");
        instance.setPort(20880);
        instance.setMetadata(Map.of(DubboConstants.INTERFACE_KEY, TEST_INTERFACE, DubboConstants.VERSION_KEY, "1.0.0",
                DubboConstants.PROTOCOL_KEY, "dubbo", "side", "provider"));
        when(sourceNamingService.getAllInstances(eq(TEST_SERVICE_NAME), eq(Constants.DEFAULT_GROUP), anyList(), eq(true)))
                .thenReturn(List.of(instance));
    }
}
