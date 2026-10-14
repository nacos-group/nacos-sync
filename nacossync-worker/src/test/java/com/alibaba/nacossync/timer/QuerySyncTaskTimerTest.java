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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
        checkTaskRows = scheduled.getAllValues().get(0);
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
    void rowReadBeforeADeleteDoesNotStartTheTaskWhileItIsBeingStopped() throws Exception {
        // deleteInBatch of 20 Eureka -> Nacos tasks in the E2E: every task kept polling, because the timer started
        // them again from rows it read while the delete was stopping them.
        AtomicBoolean rowDeleted = new AtomicBoolean();
        when(taskAccessService.findByTaskId(TASK_ID)).thenAnswer(invocation -> rowDeleted.get() ? null
                : task(TaskStatusEnum.SYNC));
        doAnswer(invocation -> {
            rowDeleted.set(true);
            return null;
        }).when(taskAccessService).deleteTaskById(TASK_ID);
        CountDownLatch stopping = new CountDownLatch(1);
        CountDownLatch stopMayFinish = new CountDownLatch(1);
        doAnswer(invocation -> {
            stopping.countDown();
            assertTrue(stopMayFinish.await(5, TimeUnit.SECONDS));
            return null;
        }).when(eventBus).post(any(DeleteTaskEvent.class));
        TaskDeleteRequest request = new TaskDeleteRequest();
        request.setTaskId(TASK_ID);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> delete = executor.submit(
                    () -> new TaskDeleteProcessor(taskAccessService, eventBus).process(request, new BaseResult()));
            assertTrue(stopping.await(5, TimeUnit.SECONDS));
            Future<?> check = executor.submit(checkTaskRows);
            Thread.sleep(300);
            stopMayFinish.countDown();
            delete.get(5, TimeUnit.SECONDS);
            check.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        verify(eventBus, never()).post(any(SyncTaskEvent.class));
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
