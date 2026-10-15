package com.alibaba.nacossync.timer;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.event.DeleteAllSubTaskEvent;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.event.SyncTaskEvent;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDeleteRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.template.processor.TaskDeleteProcessor;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CheckRunningStatusAllNacosThreadTest {

    private final TaskAccessService taskAccessService = mock(TaskAccessService.class);

    private final NacosServerHolder nacosServerHolder = mock(NacosServerHolder.class);

    private final NamingService sourceNamingService = mock(NamingService.class);

    private final SkyWalkerCacheServices skyWalkerCacheServices = mock(SkyWalkerCacheServices.class);

    private final EventBus eventBus = new EventBus();

    private final List<TaskDO> synced = new CopyOnWriteArrayList<>();

    private final List<TaskDO> deleted = new CopyOnWriteArrayList<>();

    private CheckRunningStatusAllNacosThread thread;

    private TaskDO allTask;

    /** Whether the task row is gone, as after TaskDeleteProcessor removed it. */
    private volatile boolean rowDeleted;

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
        when(taskAccessService.findByTaskId("all-task")).thenAnswer(invocation -> rowDeleted ? null : allTask);
        when(nacosServerHolder.get("source")).thenReturn(sourceNamingService);
        when(skyWalkerCacheServices.getClusterType("source")).thenReturn(ClusterTypeEnum.NACOS);
        ListView<String> services = new ListView<>();
        services.setData(List.of("svc-a"));
        when(sourceNamingService.getServicesOfServer(anyInt(), anyInt(), eq("DEFAULT_GROUP"))).thenReturn(services);
        thread = new CheckRunningStatusAllNacosThread(mock(MetricsManager.class), taskAccessService, nacosServerHolder,
                skyWalkerCacheServices, eventBus);
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

    @Test
    void allTaskWithoutAGroupSyncsTheDefaultGroup() {
        // The console sends an empty group when none is entered, and Nacos lists no service for an empty group.
        allTask.setGroupName("");
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));
        thread.run();

        assertEquals(List.of("svc-a"), synced.stream().map(TaskDO::getServiceName).toList());
    }

    @Test
    void allTaskDeletedWhileTheScanRunsIsNotStartedAgain() {
        when(taskAccessService.findAllByServiceNameEqualAll()).thenAnswer(invocation -> {
            List<TaskDO> rows = List.of(allTask);
            // The delete lands right after the scan read the rows: TaskDeleteProcessor removes the row, then
            // stops the sub-tasks.
            rowDeleted = true;
            eventBus.post(new DeleteAllSubTaskEvent(allTask));
            return rows;
        });

        thread.run();

        assertEquals(List.of(), synced.stream().map(TaskDO::getServiceName).toList());
    }

    @Test
    void aDeleteWaitsForTheScanOfTheTask() throws Exception {
        // The scan starts the sub-tasks under the task lock. A delete that comes in meanwhile must wait: otherwise it
        // finds no sub-task to stop yet, and the scan then starts them for a task that is gone.
        CountDownLatch scanning = new CountDownLatch(1);
        CountDownLatch deleteDone = new CountDownLatch(1);
        eventBus.register(new Object() {
            @Subscribe
            public void onSync(SyncTaskEvent event) throws InterruptedException {
                // Give a delete that does not wait for the scan the time to finish first.
                scanning.countDown();
                deleteDone.await(300, TimeUnit.MILLISECONDS);
            }
        });
        doAnswer(invocation -> {
            rowDeleted = true;
            return null;
        }).when(taskAccessService).deleteTaskById("all-task");
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));
        TaskDeleteRequest request = new TaskDeleteRequest();
        request.setTaskId("all-task");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> scan = executor.submit(thread);
            assertTrue(scanning.await(5, TimeUnit.SECONDS));
            Future<?> delete = executor.submit(() -> {
                new TaskDeleteProcessor(taskAccessService, eventBus).process(request, new BaseResult());
                deleteDone.countDown();
            });
            scan.get(5, TimeUnit.SECONDS);
            delete.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertDeletedSubTaskOfSvcA();
    }

    @Test
    void subTasksHaveStartedWhenTheScanLetsADeleteIn() {
        // Starting a sub-task can take a while, e.g. behind other syncs on a slow destination. A start that outlasted
        // the scan was not known to the delete that came next, and brought the sub-task up after its parent was gone.
        // The start below takes longer than the 5 s the scan used to wait for one, so this test takes a few seconds.
        List<String> order = new CopyOnWriteArrayList<>();
        eventBus.register(new Object() {
            @Subscribe
            public void onSync(SyncTaskEvent event) throws InterruptedException {
                Thread.sleep(5_500);
                order.add("sync " + event.getTaskDO().getServiceName());
            }

            @Subscribe
            public void onDelete(DeleteTaskEvent event) {
                order.add("delete " + event.getTaskDO().getServiceName());
            }
        });
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));

        thread.run();
        eventBus.post(new DeleteAllSubTaskEvent(allTask));

        assertEquals(List.of("sync svc-a", "delete svc-a"), order);
    }

    @Test
    void failedServiceListKeepsTheSubTasks() throws Exception {
        // A failed call, e.g. while the source Nacos is unreachable, says nothing about which services are gone.
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));
        thread.run();
        when(sourceNamingService.getServicesOfServer(anyInt(), anyInt(), eq("DEFAULT_GROUP")))
                .thenThrow(new NacosException(NacosException.SERVER_ERROR, "source unreachable"));

        thread.run();

        assertEquals(List.of(), deleted);
    }

    @Test
    void sourceWithoutAClientKeepsTheSubTasks() {
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));
        thread.run();
        when(nacosServerHolder.get("source")).thenReturn(null);

        thread.run();

        assertEquals(List.of(), deleted);
    }

    @Test
    void allTaskFromAnotherRegistryIsSkipped() {
        // Rejected when added since 2.0, but older rows can still be in the database. Only a Nacos source can
        // list its services; no Nacos client is created for the address of another registry.
        when(skyWalkerCacheServices.getClusterType("source")).thenReturn(ClusterTypeEnum.EUREKA);
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));

        thread.run();

        assertEquals(List.of(), synced);
        verify(nacosServerHolder, never()).get(any());
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
