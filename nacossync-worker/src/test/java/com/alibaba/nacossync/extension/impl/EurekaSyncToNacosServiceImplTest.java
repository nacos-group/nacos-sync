package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.eureka.EurekaNamingService;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author paderlol
 * @date: 2019-01-12 20:58
 * @see EurekaSyncToNacosServiceImpl
 */
@RunWith(MockitoJUnitRunner.class)
public class EurekaSyncToNacosServiceImplTest {
    public static final String TEST_SOURCE_CLUSTER_ID = "test-source-cluster-id";
    public static final String TEST_DEST_CLUSTER_ID = "test-dest-cluster-id";
    public static final String TEST_TASK_ID = "test-task-id";
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
    @InjectMocks
    @Spy
    private EurekaSyncToNacosServiceImpl eurekaSyncToNacosService;

    @Test
    public void testEurekaSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockSync(taskDO);
        Assert.assertTrue(eurekaSyncToNacosService.sync(taskDO));
    }



    @Test
    public void testEurekaDeleteSyncToNacos() throws Exception {
        TaskDO taskDO = mock(TaskDO.class);
        mockDelete(taskDO);
        Assert.assertTrue(eurekaSyncToNacosService.delete(taskDO));
    }
    @Test(expected = Exception.class)
    public void testEurekaSyncToNacosWithException() throws Exception {
        Assert.assertFalse(eurekaSyncToNacosService.sync(null));
    }
    @Test(expected = Exception.class)
    public void testEurekaDeleteToNacosWithException() throws Exception {
        Assert.assertFalse(eurekaSyncToNacosService.delete(null));
    }

    public void mockSync(TaskDO taskDO) throws Exception {


        Application application = mock(Application.class);
        InstanceInfo instanceInfoUp = InstanceInfo.Builder.newBuilder().setIPAddr("127.0.0.1").setPort(8080)
                .setStatus(InstanceInfo.InstanceStatus.UP).setAppName("SPRING-CLOUD-EUREKA-CLIENT").build();
        InstanceInfo instanceInfoDown = InstanceInfo.Builder.newBuilder().setIPAddr("127.0.0.1").setPort(8081)
                .setStatus(InstanceInfo.InstanceStatus.DOWN).setAppName("SPRING-CLOUD-EUREKA-CLIENT").build();
        List<InstanceInfo> allInstanceInfo = Lists.newArrayList(instanceInfoUp, instanceInfoDown);
        when(taskDO.getTaskId()).thenReturn(TEST_TASK_ID);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);

        doReturn(destNamingService).when(nacosServerHolder).get(anyString(), any());
        doReturn(eurekaNamingService).when(eurekaServerHolder).get(anyString(), any());
        doReturn(allInstanceInfo).when(eurekaNamingService).getApplications(any());
        when(application.getInstances()).thenReturn(allInstanceInfo);
        doReturn(ClusterTypeEnum.EUREKA).when(skyWalkerCacheServices).getClusterType(any());
    }

    public void mockDelete(TaskDO taskDO) throws Exception {
        Instance instance = mock(Instance.class);
        when(taskDO.getTaskId()).thenReturn(TEST_TASK_ID);
        when(taskDO.getSourceClusterId()).thenReturn(TEST_SOURCE_CLUSTER_ID);
        when(taskDO.getDestClusterId()).thenReturn(TEST_DEST_CLUSTER_ID);
        doReturn(destNamingService).when(nacosServerHolder).get(anyString(), anyString());
        Map<String, String> metadata = Maps.newHashMap();
        metadata.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, TEST_SOURCE_CLUSTER_ID);
        List<Instance> allInstances = Lists.newArrayList(instance);
        doReturn(allInstances).when(destNamingService).getAllInstances(anyString());
        doReturn(metadata).when(instance).getMetadata();
    }

}
