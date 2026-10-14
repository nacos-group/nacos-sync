package com.alibaba.nacossync.extension.event;

import com.alibaba.nacossync.extension.event.listener.SpecialSyncEventListener;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.eventbus.EventBus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * A delete unsubscribes the task and then removes the instances it synced. A poll of that task must not register them
 * again afterwards.
 */
class SpecialSyncEventBusTest {

    private final SpecialSyncEventBus specialSyncEventBus = new SpecialSyncEventBus();

    private final SpecialSyncEventListener listener = new SpecialSyncEventListener(mock(EventBus.class));

    private final TaskDO taskDO = task();

    @Test
    void pollAfterUnsubscribeDoesNothing() {
        // The timer took the poll from the registry just before the delete unsubscribed the task.
        AtomicInteger polls = new AtomicInteger();
        specialSyncEventBus.subscribe(taskDO, task -> polls.incrementAndGet());
        SpecialSyncEvent poll = specialSyncEventBus.getAllSpecialSyncEvent().iterator().next();

        specialSyncEventBus.unsubscribe(taskDO);
        listener.listenerSpecialSyncEvent(poll);

        assertEquals(0, polls.get());
    }

    @Test
    void unsubscribeWaitsForARunningPoll() throws Exception {
        // E2E: a poll of ELOAD-5 registered its instances while the delete was removing them, so they stayed.
        CountDownLatch polling = new CountDownLatch(1);
        CountDownLatch pollMayFinish = new CountDownLatch(1);
        specialSyncEventBus.subscribe(taskDO, task -> {
            polling.countDown();
            await(pollMayFinish);
        });
        SpecialSyncEvent poll = specialSyncEventBus.getAllSpecialSyncEvent().iterator().next();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> running = executor.submit(() -> listener.listenerSpecialSyncEvent(poll));
            assertTrue(polling.await(5, TimeUnit.SECONDS));
            Future<?> unsubscribe = executor.submit(() -> specialSyncEventBus.unsubscribe(taskDO));
            Thread.sleep(300);
            assertFalse(unsubscribe.isDone(), "unsubscribe returned while the poll was still registering");

            pollMayFinish.countDown();
            running.get(5, TimeUnit.SECONDS);
            unsubscribe.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static TaskDO task() {
        TaskDO taskDO = new TaskDO();
        taskDO.setTaskId("task-id");
        taskDO.setServiceName("ELOAD-5");
        return taskDO;
    }
}
