package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.*;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author paderlol
 * @date: 2019-01-12 14:39
 * @see ZookeeperSyncToNacosServiceImpl
 */
@RunWith(MockitoJUnitRunner.class)
public class ZookeeperSyncToNacosServiceImplTest {

    private static final String TEST_PATH =
        "/dubbo/org.apache.dubbo.demo.DemoService/providers/hessian%3A%2F%2F172.16.0.10%3A20880%2Forg.apache.dubbo.demo.DemoService%3Fanyhost%3Dtrue%26application%3Ddemo-provider%26dubbo%3D2.0.2%26generic%3Dfalse%26group%3DtestGroup%26interface%3Dorg.apache.dubbo.demo.DemoService%26methods%3DsayHello%26pid%3D5956%26revision%3D1.0.0%26side%3Dprovider%26timestamp%3D1547285978821%26version%3D1.0.0%26weight%3D1";
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
    private TreeCache treeCache;
    @InjectMocks
    @Spy
    private ZookeeperSyncToNacosServiceImpl zookeeperSyncToNacosService;

    @Test
    public void testZookeeperSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        Assert.assertTrue(mockSync(taskDO));

    }

    @Test
    public void testZookeeperDeleteToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockSync(taskDO);
        zookeeperSyncToNacosService.sync(taskDO);

        Assert.assertTrue(mockDelete(taskDO));

    }

    @Test(expected = Exception.class)
    public void tesZookeeperSyncToNacosWithException() throws Exception {
        Assert.assertFalse(zookeeperSyncToNacosService.sync(null));
    }

    @Test(expected = Exception.class)
    public void testZookeeperDeleteToNacosWithException() throws Exception {
        Assert.assertFalse(zookeeperSyncToNacosService.delete(null));
    }

    public boolean mockSync(TaskDO taskDO) throws Exception {
        ChildData childData = new ChildData(TEST_PATH, null, null);
        ListenerContainer<TreeCacheListener> listeners = mock(ListenerContainer.class);
        when(taskDO.getTaskId()).thenReturn(TEST_TASK_ID);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        CuratorFramework curatorFramework = mock(CuratorFramework.class);
        doReturn(curatorFramework).when(zookeeperServerHolder).get(any(), any());
        doReturn(destNamingService).when(nacosServerHolder).get(any(), any());
        doReturn(treeCache).when(zookeeperSyncToNacosService).getTreeCache(any());
        when(treeCache.getCurrentData(any())).thenReturn(childData);
        doReturn(ClusterTypeEnum.ZK).when(skyWalkerCacheServices).getClusterType(any());
        when(treeCache.getListenable()).thenReturn(listeners);
        return zookeeperSyncToNacosService.sync(taskDO);
    }

    public boolean mockDelete(TaskDO taskDO) throws Exception {
        doNothing().when(treeCache).close();
        Instance instance = mock(Instance.class);
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, TEST_SOURCE_CLUSTER_ID);
        List<Instance> allInstances = Lists.newArrayList(instance);

        doReturn(allInstances).when(destNamingService).getAllInstances(anyString());

        doReturn(metadata).when(instance).getMetadata();
        // verify(destNamingService).deregisterInstance(anyString(), anyString(), anyInt());
        return zookeeperSyncToNacosService.delete(taskDO);
    }

}
