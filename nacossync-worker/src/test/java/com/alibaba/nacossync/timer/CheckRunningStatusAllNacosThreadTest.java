package com.alibaba.nacossync.timer;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.event.DeleteAllSubTaskEvent;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.event.SyncTaskEvent;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CheckRunningStatusAllNacosThreadTest {

    private final TaskAccessService taskAccessService = mock(TaskAccessService.class);

    private final NacosServerHolder nacosServerHolder = mock(NacosServerHolder.class);

    private final NamingService sourceNamingService = mock(NamingService.class);

    private final EventBus eventBus = new EventBus();

    private final List<TaskDO> synced = new CopyOnWriteArrayList<>();

    private final List<TaskDO> deleted = new CopyOnWriteArrayList<>();

    private CheckRunningStatusAllNacosThread thread;

    private TaskDO allTask;

    @BeforeEach
    void setUp() throws Exception {
        eventBus.register(new Object() {
            @Subscribe
            public void onSync(SyncTaskEvent event) {
                synced.add(event.getTaskDO());
            }

            @Subscribe
            public void onDelete(DeleteTaskEvent event) {
                deleted.add(event.getTaskDO());
            }
        });
        allTask = new TaskDO();
        allTask.setTaskId("all-task");
        allTask.setServiceName("ALL");
        allTask.setGroupName("DEFAULT_GROUP");
        allTask.setSourceClusterId("source");
        allTask.setDestClusterId("dest");
        allTask.setTaskStatus(TaskStatusEnum.SYNC.getCode());
        when(nacosServerHolder.get("source")).thenReturn(sourceNamingService);
        ListView<String> services = new ListView<>();
        services.setData(List.of("svc-a"));
        when(sourceNamingService.getServicesOfServer(anyInt(), anyInt(), eq("DEFAULT_GROUP"))).thenReturn(services);
        thread = new CheckRunningStatusAllNacosThread(mock(MetricsManager.class), taskAccessService, nacosServerHolder,
                eventBus);
    }

    @Test
    void deletingAnAllTaskDeletesItsSubTasks() {
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));
        thread.run();
        assertEquals(List.of("svc-a"), synced.stream().map(TaskDO::getServiceName).toList());

        // TaskDeleteProcessor posts this before removing the task row.
        eventBus.post(new DeleteAllSubTaskEvent(allTask));

        assertDeletedSubTaskOfSvcA();
    }

    @Test
    void subTasksOfAnAllTaskRemovedFromTheDatabaseAreDeletedWithTheirClusters() {
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask)).thenReturn(List.of());
        thread.run();
        // The task row is gone, for example after a batch delete that posts no event.
        thread.run();

        assertDeletedSubTaskOfSvcA();
    }

    private void assertDeletedSubTaskOfSvcA() {
        assertEquals(1, deleted.size());
        TaskDO subTask = deleted.get(0);
        assertEquals("svc-a", subTask.getServiceName());
        assertEquals("source", subTask.getSourceClusterId());
        assertEquals("dest", subTask.getDestClusterId());
        assertEquals(synced.get(0).getTaskId(), subTask.getTaskId());
    }
}
