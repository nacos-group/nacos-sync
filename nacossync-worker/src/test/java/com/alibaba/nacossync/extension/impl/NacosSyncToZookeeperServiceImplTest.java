package com.alibaba.nacossync.extension.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

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
    @Mock
    private ZookeeperServerHolder zookeeperServerHolder;
    @Mock
    private NacosServerHolder nacosServerHolder;
    @Mock
    private NamingService sourceNamingService;
    @Mock
    private PathChildrenCache pathChildrenCache;
    @Mock
    private CuratorFramework client;

    @InjectMocks
    @Spy
    private NacosSyncToZookeeperServiceImpl nacosSyncToZookeeperService;

    @Test
    public void testNacosSyncToZookeeper() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        Assert.assertTrue(mockSync(taskDO));
    }

    @Test
    public void testNacosDeleteToZookeeper() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockSync(taskDO);
        //TODO Test the core logic in the future
//        Assert.assertTrue(mockDelete(taskDO));
    }

    @Test(expected = Exception.class)
    public void testNacosSyncToZookeeperWithException() throws Exception {
        Assert.assertFalse(nacosSyncToZookeeperService.sync(null,null));
    }
    @Test(expected = Exception.class)
    public void testNacosDeleteToZookeeperWithException() throws Exception {
        Assert.assertFalse(nacosSyncToZookeeperService.delete(null));
    }

    public boolean mockSync(TaskDO taskDO) throws Exception {
        when(taskDO.getTaskId()).thenReturn(TEST_TASK_ID);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        doReturn(client).when(zookeeperServerHolder).get(any());
        doReturn(sourceNamingService).when(nacosServerHolder).get(any());

        //TODO Test the core logic in the future
        return nacosSyncToZookeeperService.sync(taskDO, null);
    }

    public boolean mockDelete(TaskDO taskDO) throws Exception {


        return nacosSyncToZookeeperService.delete(taskDO);
    }
}
