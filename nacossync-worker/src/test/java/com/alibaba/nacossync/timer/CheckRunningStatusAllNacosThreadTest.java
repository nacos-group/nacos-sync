package com.alibaba.nacossync.timer;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.extension.TaskDispatcher;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDeleteRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.template.processor.TaskDeleteProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CheckRunningStatusAllNacosThreadTest {

    private final TaskAccessService taskAccessService = mock(TaskAccessService.class);

    private final NacosServerHolder nacosServerHolder = mock(NacosServerHolder.class);

    private final NamingService sourceNamingService = mock(NamingService.class);

    private final SkyWalkerCacheServices skyWalkerCacheServices = mock(SkyWalkerCacheServices.class);

    private final TaskDispatcher taskDispatcher = mock(TaskDispatcher.class);

    private final List<TaskDO> synced = new CopyOnWriteArrayList<>();

    private final List<TaskDO> deleted = new CopyOnWriteArrayList<>();

    private CheckRunningStatusAllNacosThread thread;

    private TaskDO allTask;

    /** Whether the task row is gone, as after TaskDeleteProcessor removed it. */
    private volatile boolean rowDeleted;

    @BeforeEach
    void setUp() throws Exception {
        doAnswer(invocation -> synced.add(invocation.getArgument(0))).when(taskDispatcher).sync(any());
        doAnswer(invocation -> deleted.add(invocation.getArgument(0))).when(taskDispatcher).delete(any());
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
        when(sourceNamingService.getServicesOfServer(anyInt(), anyInt(), eq("DEFAULT_GROUP")))
                .thenReturn(services("svc-a"));
        thread = new CheckRunningStatusAllNacosThread(mock(MetricsManager.class), taskAccessService, nacosServerHolder,
                skyWalkerCacheServices, taskDispatcher);
    }

    @Test
    void deletingAnAllTaskDeletesItsSubTasks() {
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));
        thread.run();
        assertEquals(List.of("svc-a"), synced.stream().map(TaskDO::getServiceName).toList());

        // TaskDeleteProcessor removes the task row, then calls this.
        thread.deleteAllSubTasks(allTask);

        assertDeletedSubTaskOfSvcA();
    }

    @Test
    void subTasksOfAnAllTaskRemovedFromTheDatabaseAreDeletedWithTheirClusters() {
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask)).thenReturn(List.of());
        thread.run();
        // The task row is gone without the delete reaching this node, e.g. deleted through another nacos-sync node.
        thread.run();

        assertDeletedSubTaskOfSvcA();
    }

    @Test
    void subTaskThatFailedToStartIsStartedAgain() {
        // A start that failed, e.g. while the destination was down, is tried again in the next round, as for any
        // other task. Taken as started, it never was: a Nacos to ZooKeeper sub-task, with no full sync behind it,
        // never synced.
        doReturn(false).doReturn(true).when(taskDispatcher).sync(any());
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));

        thread.run();
        thread.run();
        thread.run();

        verify(taskDispatcher, times(2)).sync(any());
    }

    @Test
    void anErrorFromASubTaskDoesNotStopTheScan() {
        // The scan runs on a fixed-delay schedule, which stops for good, without a word, once a run throws. Starting
        // the sub-tasks on a pool used to keep an Error, e.g. a NoClassDefFoundError from a client library, away
        // from it.
        doThrow(new NoClassDefFoundError("test")).doAnswer(invocation -> synced.add(invocation.getArgument(0)))
                .when(taskDispatcher).sync(any());
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));

        assertDoesNotThrow(thread::run);
        thread.run();

        assertEquals(List.of("svc-a"), synced.stream().map(TaskDO::getServiceName).toList());
    }

    @Test
    void subTaskThatFailedToStopIsStoppedAgain() throws Exception {
        // The service is gone from the source. Taken as stopped, the sub-task that failed to stop kept its copies.
        when(sourceNamingService.getServicesOfServer(anyInt(), anyInt(), eq("DEFAULT_GROUP")))
                .thenReturn(services("svc-a")).thenReturn(services());
        doReturn(false).doReturn(true).when(taskDispatcher).delete(any());
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));

        thread.run();
        thread.run();
        thread.run();
        thread.run();

        verify(taskDispatcher, times(2)).delete(any());
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
            thread.deleteAllSubTasks(allTask);
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
        doAnswer(invocation -> {
            synced.add(invocation.getArgument(0));
            // Give a delete that does not wait for the scan the time to finish first.
            scanning.countDown();
            deleteDone.await(300, TimeUnit.MILLISECONDS);
            return true;
        }).when(taskDispatcher).sync(any());
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
                new TaskDeleteProcessor(taskAccessService, taskDispatcher, thread).process(request, new BaseResult());
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
    void taskBeingDeletedIsSkippedRatherThanWaitedFor() throws Exception {
        CountDownLatch deleting = new CountDownLatch(1);
        CountDownLatch deleteMayFinish = new CountDownLatch(1);
        doAnswer(invocation -> {
            deleting.countDown();
            assertTrue(deleteMayFinish.await(5, TimeUnit.SECONDS));
            return null;
        }).when(taskAccessService).deleteTaskById("all-task");
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));
        TaskDeleteRequest request = new TaskDeleteRequest();
        request.setTaskId("all-task");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> delete = executor.submit(
                    () -> new TaskDeleteProcessor(taskAccessService, taskDispatcher, thread).process(request,
                            new BaseResult()));
            assertTrue(deleting.await(5, TimeUnit.SECONDS));

            assertTimeoutPreemptively(Duration.ofSeconds(1), thread::run);

            deleteMayFinish.countDown();
            delete.get(5, TimeUnit.SECONDS);
        } finally {
            deleteMayFinish.countDown();
            executor.shutdownNow();
        }
        assertEquals(List.of(), synced);
    }

    @Test
    void subTasksHaveStartedWhenTheScanLetsADeleteIn() {
        // Starting a sub-task can take a while, e.g. behind other syncs on a slow destination. A start that outlasted
        // the scan was not known to the delete that came next, and brought the sub-task up after its parent was gone.
        // The start below takes longer than the 5 s the scan used to wait for one, so this test takes a few seconds.
        List<String> order = new CopyOnWriteArrayList<>();
        doAnswer(invocation -> {
            Thread.sleep(5_500);
            order.add("sync " + invocation.<TaskDO>getArgument(0).getServiceName());
            return true;
        }).when(taskDispatcher).sync(any());
        doAnswer(invocation -> order.add("delete " + invocation.<TaskDO>getArgument(0).getServiceName()))
                .when(taskDispatcher).delete(any());
        when(taskAccessService.findAllByServiceNameEqualAll()).thenReturn(List.of(allTask));

        thread.run();
        thread.deleteAllSubTasks(allTask);

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

    private static ListView<String> services(String... names) {
        ListView<String> services = new ListView<>();
        services.setData(List.of(names));
        return services;
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
