package com.alibaba.nacossync.timer;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.event.SyncTaskEvent;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDeleteRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.template.processor.TaskDeleteProcessor;
import com.google.common.eventbus.EventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private final EventBus eventBus = mock(EventBus.class);

    private final ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);

    private Runnable checkTaskRows;

    @BeforeEach
    void setUp() {
        new QuerySyncTaskTimer(mock(MetricsManager.class), skyWalkerCacheServices, taskAccessService, eventBus,
                scheduledExecutorService, mock(NacosServerHolder.class)).run();
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

        verify(eventBus, never()).post(any(SyncTaskEvent.class));
    }

    @Test
    void taskPausedAfterTheRowsWereReadIsNotStarted() {
        when(taskAccessService.findByTaskId(TASK_ID)).thenReturn(task(TaskStatusEnum.DELETE));

        checkTaskRows.run();

        verify(eventBus, never()).post(any(SyncTaskEvent.class));
    }

    @Test
    void taskStillInTheDatabaseIsStarted() {
        when(taskAccessService.findByTaskId(TASK_ID)).thenReturn(task(TaskStatusEnum.SYNC));

        checkTaskRows.run();

        verify(eventBus).post(any(SyncTaskEvent.class));
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
        when(taskAccessService.findByTaskId(TASK_ID)).thenAnswer(invocation -> {
            TaskDO row = rowDeleted.get() ? null : task(TaskStatusEnum.SYNC);
            if (reads.getAndIncrement() == 0) {
                // The timer has read the row: give a delete that does not wait for it the time to finish first.
                rowRead.countDown();
                deleteDone.await(300, TimeUnit.MILLISECONDS);
            }
            return row;
        });
        doAnswer(invocation -> {
            rowDeleted.set(true);
            return null;
        }).when(taskAccessService).deleteTaskById(TASK_ID);
        doAnswer(invocation -> order.add("sync")).when(eventBus).post(any(SyncTaskEvent.class));
        doAnswer(invocation -> order.add("delete")).when(eventBus).post(any(DeleteTaskEvent.class));
        TaskDeleteRequest request = new TaskDeleteRequest();
        request.setTaskId(TASK_ID);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> check = executor.submit(checkTaskRows);
            assertTrue(rowRead.await(5, TimeUnit.SECONDS));
            Future<?> delete = executor.submit(() -> {
                new TaskDeleteProcessor(taskAccessService, eventBus).process(request, new BaseResult());
                deleteDone.countDown();
            });
            check.get(5, TimeUnit.SECONDS);
            delete.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertEquals(List.of("sync", "delete"), order);
    }

    private static TaskDO task(TaskStatusEnum status) {
        TaskDO taskDO = new TaskDO();
        taskDO.setTaskId(TASK_ID);
        taskDO.setServiceName("ELOAD-0");
        taskDO.setOperationId("operation-id");
        taskDO.setTaskStatus(status.getCode());
        return taskDO;
    }
}
