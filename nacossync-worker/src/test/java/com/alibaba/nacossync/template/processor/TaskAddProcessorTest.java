package com.alibaba.nacossync.template.processor;

import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskAddRequest;
import com.alibaba.nacossync.pojo.result.TaskAddResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskAddProcessorTest {

    private final SyncManagerService syncManagerService = mock(SyncManagerService.class);

    private final TaskAccessService taskAccessService = mock(TaskAccessService.class);

    private final ClusterAccessService clusterAccessService = mock(ClusterAccessService.class);

    private final TaskAddProcessor processor = new TaskAddProcessor(syncManagerService, taskAccessService,
            clusterAccessService);

    @ParameterizedTest
    @EnumSource(value = ClusterTypeEnum.class, names = {"EUREKA", "CONSUL", "ZK"})
    void allTaskFromAnotherRegistryIsRejected(ClusterTypeEnum sourceType) {
        // Only a Nacos source can list its services for ALL. Such a task used to be added, then never synced:
        // the sync timer skips every task named ALL and the ALL thread treats the source as a Nacos server.
        TaskAddRequest request = request(sourceType, "all");

        assertThrows(SkyWalkerException.class, () -> processor.process(request, new TaskAddResult()));

        verify(taskAccessService, never()).addTask(any());
    }

    @Test
    void allTaskFromNacosIsAdded() throws Exception {
        processor.process(request(ClusterTypeEnum.NACOS, "ALL"), new TaskAddResult());

        ArgumentCaptor<TaskDO> added = ArgumentCaptor.forClass(TaskDO.class);
        verify(taskAccessService).addTask(added.capture());
        assertEquals("ALL", added.getValue().getServiceName());
    }

    @Test
    void wildcardTaskFromZookeeperIsAdded() throws Exception {
        processor.process(request(ClusterTypeEnum.ZK, "*"), new TaskAddResult());

        verify(taskAccessService).addTask(any());
    }

    private TaskAddRequest request(ClusterTypeEnum sourceType, String serviceName) {
        when(clusterAccessService.findByClusterId("source")).thenReturn(cluster("source", sourceType));
        when(clusterAccessService.findByClusterId("dest")).thenReturn(cluster("dest", ClusterTypeEnum.NACOS));
        when(syncManagerService.getSyncService(anyString(), anyString())).thenReturn(mock(SyncService.class));
        TaskAddRequest request = new TaskAddRequest();
        request.setSourceClusterId("source");
        request.setDestClusterId("dest");
        request.setServiceName(serviceName);
        return request;
    }

    private static ClusterDO cluster(String clusterId, ClusterTypeEnum type) {
        ClusterDO cluster = new ClusterDO();
        cluster.setClusterId(clusterId);
        cluster.setClusterType(type.getCode());
        return cluster;
    }
}
