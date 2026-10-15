package com.alibaba.nacossync.timer;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.extension.TaskDispatcher;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDeleteRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.template.processor.TaskDeleteProcessor;
import com.alibaba.nacossync.util.TaskLocks;
import com.alibaba.nacossync.util.ThreadWaits;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The sync timer reads the task rows and then starts the tasks without a finished sync one by one. A task deleted or
 * paused in between must not be started from the row read before.
 */
class QuerySyncTaskTimerTest {

    private static final String TASK_ID = "task-id";

    private final SkyWalkerCacheServices skyWalkerCacheServices = mock(SkyWalkerCacheServices.class);

    private final TaskAccessService taskAccessService = mock(TaskAccessService.class);

    private final TaskDispatcher taskDispatcher = mock(TaskDispatcher.class);

    private final CheckRunningStatusAllNacosThread checkRunningStatusAllNacosThread =
            mock(CheckRunningStatusAllNacosThread.class);

    private final ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);

    private Runnable checkTaskRows;

    @BeforeEach
    void setUp() {
        new QuerySyncTaskTimer(mock(MetricsManager.class), skyWalkerCacheServices, taskAccessService,
                taskDispatcher, scheduledExecutorService, checkRunningStatusAllNacosThread).run();
        ArgumentCaptor<Runnable> scheduled = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduledExecutorService, times(2)).scheduleWithFixedDelay(scheduled.capture(), anyLong(), anyLong(),
                any(TimeUnit.class));
        // The other one is the ALL scan.
        checkTaskRows = scheduled.getAllValues().stream()
                .filter(runnable -> !(runnable instanceof CheckRunningStatusAllNacosThread)).findFirst().orElseThrow();
        when(taskAccessService.findAllByServiceNameNotEqualAll()).thenReturn(List.of(task(TaskStatusEnum.SYNC)));
    }

    @Test
    void taskDeletedAfterTheRowsWereReadIsNotStarted() {
        checkTaskRows.run();

        verify(taskDispatcher, never()).sync(any());
    }

    @Test
    void taskPausedAfterTheRowsWereReadIsNotStarted() {
        when(taskAccessService.findByTaskId(TASK_ID)).thenReturn(task(TaskStatusEnum.DELETE));

        checkTaskRows.run();

        verify(taskDispatcher, never()).sync(any());
    }

    @Test
    void taskStillInTheDatabaseIsStarted() {
        when(taskAccessService.findByTaskId(TASK_ID)).thenReturn(task(TaskStatusEnum.SYNC));

        checkTaskRows.run();

        verify(taskDispatcher).sync(any());
    }

    @Test
    void aDeleteWaitsForAStartInProgress() throws Exception {
        // The timer re-reads the row and starts the task under the task lock. A delete that comes in between must
        // wait: otherwise it stops the task first, the timer then starts it from the row it had read, and the deleted
        // task keeps running.
        List<String> order = new CopyOnWriteArrayList<>();
        AtomicBoolean rowDeleted = new AtomicBoolean();
        AtomicInteger reads = new AtomicInteger();
        CountDownLatch rowRead = new CountDownLatch(1);
        CountDownLatch deleteDone = new CountDownLatch(1);
        AtomicReference<Thread> deleter = new AtomicReference<>();
        when(taskAccessService.findByTaskId(TASK_ID)).thenAnswer(invocation -> {
            TaskDO row = rowDeleted.get() ? null : task(TaskStatusEnum.SYNC);
            if (reads.getAndIncrement() == 0) {
                // The timer has read the row: let a delete that does not wait for it finish first.
                rowRead.countDown();
                ThreadWaits.awaitUntil(() -> deleteDone.getCount() == 0
                                || ThreadWaits.isWaitingIn(deleter.get(), TaskLocks.class, "runLocked"),
                        "the delete neither finished nor waited for the task lock");
            }
            return row;
        });
        doAnswer(invocation -> {
            rowDeleted.set(true);
            return null;
        }).when(taskAccessService).deleteTaskById(TASK_ID);
        doAnswer(invocation -> order.add("sync")).when(taskDispatcher).sync(any());
        doAnswer(invocation -> order.add("delete")).when(taskDispatcher).delete(any());
        TaskDeleteRequest request = new TaskDeleteRequest();
        request.setTaskId(TASK_ID);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> check = executor.submit(checkTaskRows);
            assertTrue(rowRead.await(5, TimeUnit.SECONDS));
            Future<?> delete = executor.submit(() -> {
                deleter.set(Thread.currentThread());
                new TaskDeleteProcessor(taskAccessService, taskDispatcher, checkRunningStatusAllNacosThread)
                        .process(request, new BaseResult());
                deleteDone.countDown();
            });
            check.get(5, TimeUnit.SECONDS);
            delete.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertEquals(List.of("sync", "delete"), order);
    }

    @Test
    void taskBeingDeletedIsSkippedRatherThanWaitedFor() throws Exception {
        // The timer goes through all tasks on one thread. Waiting for the lock of a task that is being deleted, or that
        // shares a lock stripe with a slow one, would hold up every task after it; the next round comes in 3 s.
        TaskDO other = task("other-task-id", TaskStatusEnum.SYNC);
        when(taskAccessService.findAllByServiceNameNotEqualAll()).thenReturn(List.of(task(TaskStatusEnum.SYNC), other));
        when(taskAccessService.findByTaskId(TASK_ID)).thenReturn(task(TaskStatusEnum.SYNC));
        when(taskAccessService.findByTaskId("other-task-id")).thenReturn(other);
        CountDownLatch deleting = new CountDownLatch(1);
        CountDownLatch deleteMayFinish = new CountDownLatch(1);
        doAnswer(invocation -> {
            deleting.countDown();
            assertTrue(deleteMayFinish.await(5, TimeUnit.SECONDS));
            return null;
        }).when(taskAccessService).deleteTaskById(TASK_ID);
        TaskDeleteRequest request = new TaskDeleteRequest();
        request.setTaskId(TASK_ID);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> delete = executor.submit(
                    () -> new TaskDeleteProcessor(taskAccessService, taskDispatcher, checkRunningStatusAllNacosThread)
                            .process(request, new BaseResult()));
            assertTrue(deleting.await(5, TimeUnit.SECONDS));

            assertTimeoutPreemptively(Duration.ofSeconds(1), checkTaskRows::run);

            deleteMayFinish.countDown();
            delete.get(5, TimeUnit.SECONDS);
        } finally {
            deleteMayFinish.countDown();
            executor.shutdownNow();
        }
        // The task after it, on another lock stripe, has started.
        ArgumentCaptor<TaskDO> started = ArgumentCaptor.forClass(TaskDO.class);
        verify(taskDispatcher).sync(started.capture());
        assertEquals("other-task-id", started.getValue().getTaskId());
    }

    private static TaskDO task(TaskStatusEnum status) {
        return task(TASK_ID, status);
    }

    private static TaskDO task(String taskId, TaskStatusEnum status) {
        TaskDO taskDO = new TaskDO();
        taskDO.setTaskId(taskId);
        taskDO.setServiceName("ELOAD-0");
        taskDO.setOperationId("operation-id");
        taskDO.setTaskStatus(status.getCode());
        return taskDO;
    }
}
