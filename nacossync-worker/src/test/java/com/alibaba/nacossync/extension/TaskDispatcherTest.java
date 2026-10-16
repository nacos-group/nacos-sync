package com.alibaba.nacossync.extension;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.ThreadWaits;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Syncs and deletes ran as Guava EventBus subscribers, which are called by one thread at a time. The timers and the
 * delete requests call them from different threads, and the sync services have never run concurrently.
 */
class TaskDispatcherTest {

    private final SyncManagerService syncManagerService = mock(SyncManagerService.class);

    private final TaskDispatcher taskDispatcher = new TaskDispatcher(mock(MetricsManager.class), syncManagerService,
            mock(SkyWalkerCacheServices.class));

    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void oneSyncAtATime() throws Exception {
        CountDownLatch syncing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        when(syncManagerService.sync(any(), any())).thenAnswer(invocation -> {
            calls.incrementAndGet();
            syncing.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return true;
        });

        Future<?> first = executor.submit(() -> taskDispatcher.sync(task("a")));
        assertTrue(syncing.await(5, TimeUnit.SECONDS));
        AtomicReference<Thread> secondThread = new AtomicReference<>();
        Future<?> second = executor.submit(() -> {
            secondThread.set(Thread.currentThread());
            taskDispatcher.sync(task("b"));
        });
        ThreadWaits.awaitWaitingIn(secondThread, TaskDispatcher.class, "sync", "the second sync did not wait");
        assertEquals(1, calls.get());
        release.countDown();
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals(2, calls.get());
    }

    @Test
    void oneDeleteAtATime() throws Exception {
        CountDownLatch deleting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        when(syncManagerService.delete(any())).thenAnswer(invocation -> {
            calls.incrementAndGet();
            deleting.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return true;
        });

        Future<?> first = executor.submit(() -> taskDispatcher.delete(task("a")));
        assertTrue(deleting.await(5, TimeUnit.SECONDS));
        AtomicReference<Thread> secondThread = new AtomicReference<>();
        Future<?> second = executor.submit(() -> {
            secondThread.set(Thread.currentThread());
            taskDispatcher.delete(task("b"));
        });
        ThreadWaits.awaitWaitingIn(secondThread, TaskDispatcher.class, "delete", "the second delete did not wait");
        assertEquals(1, calls.get());
        release.countDown();
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        assertEquals(2, calls.get());
    }

    @Test
    void deleteDoesNotWaitForASync() throws Exception {
        CountDownLatch syncing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(syncManagerService.sync(any(), any())).thenAnswer(invocation -> {
            syncing.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return true;
        });
        when(syncManagerService.delete(any())).thenReturn(true);

        Future<?> sync = executor.submit(() -> taskDispatcher.sync(task("a")));
        assertTrue(syncing.await(5, TimeUnit.SECONDS));
        executor.submit(() -> taskDispatcher.delete(task("b"))).get(2, TimeUnit.SECONDS);
        verify(syncManagerService).delete(any());
        release.countDown();
        sync.get(5, TimeUnit.SECONDS);
    }

    @Test
    void tellsWhetherATaskStartedOrStopped() throws Exception {
        // The ALL scan tries a sub-task again in the next round when it did not start or stop.
        when(syncManagerService.sync(any(), any())).thenReturn(true, false)
                .thenThrow(new IllegalStateException("test"));
        when(syncManagerService.delete(any())).thenReturn(true, false)
                .thenThrow(new IllegalStateException("test"));
        TaskDO a = task("a");

        assertEquals(List.of(true, false, false),
                List.of(taskDispatcher.sync(a), taskDispatcher.sync(a), taskDispatcher.sync(a)));
        assertEquals(List.of(true, false, false),
                List.of(taskDispatcher.delete(a), taskDispatcher.delete(a), taskDispatcher.delete(a)));
    }

    private static TaskDO task(String taskId) {
        TaskDO taskDO = new TaskDO();
        taskDO.setTaskId(taskId);
        return taskDO;
    }
}
