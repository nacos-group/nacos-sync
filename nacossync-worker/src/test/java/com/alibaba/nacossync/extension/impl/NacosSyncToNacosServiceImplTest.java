package com.alibaba.nacossync.extension.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
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
 * @see NacosSyncToNacosServiceImpl
 */
@RunWith(MockitoJUnitRunner.class)
public class NacosSyncToNacosServiceImplTest {
    public static final String TEST_SOURCE_CLUSTER_ID = "test-source-cluster-id";
    public static final String TEST_DEST_CLUSTER_ID = "test-dest-cluster-id";
    public static final String TEST_TASK_ID = "test-task-id";
    @Mock
    private NacosServerHolder nacosServerHolder;
    @Mock
    private NamingService sourceNamingService;

    @Mock
    private NamingService destNamingService;

    @InjectMocks
    @Spy
    private NacosSyncToNacosServiceImpl nacosSyncToNacosService;

    @Test
    public void testZookeeperSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockSync(taskDO);
        // TODO Test the core logic in the future
        Assert.assertTrue(nacosSyncToNacosService.sync(taskDO,null));
    }
    
    @Test
    public void testZookeeperSyncToNacosWithTimeSync() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        try {
            nacosSyncToNacosService.timeSync(taskDO);
        }catch (Exception e) {
            Assert.assertEquals(e, NacosException.class);
        }
    }
    
    @Test(expected = Exception.class)
    public void testZookeeperSyncToNacosWithTimeSync2() throws Exception {
        nacosSyncToNacosService.timeSync(null);
    }

    @Test
    public void testDeleteSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockDelete(taskDO);
        // TODO Test the core logic in the future
        Assert.assertTrue(nacosSyncToNacosService.delete(taskDO));
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
        doReturn(destNamingService).when(nacosServerHolder).get(anyString());
        doReturn(sourceNamingService).when(nacosServerHolder).get(anyString());
    }

    public void mockDelete(TaskDO taskDO) throws Exception {
        Instance instance = mock(Instance.class);
        when(taskDO.getTaskId()).thenReturn(TEST_TASK_ID);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        doReturn(destNamingService).when(nacosServerHolder).get(anyString());
        doReturn(sourceNamingService).when(nacosServerHolder).get(anyString());
        doNothing().when(sourceNamingService).unsubscribe(any(), any());
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, TEST_SOURCE_CLUSTER_ID);
        List<Instance> allInstances = Lists.newArrayList(instance);
        doReturn(allInstances).when(sourceNamingService).getAllInstances(anyString());
        doReturn(metadata).when(instance).getMetadata();
    }

}
