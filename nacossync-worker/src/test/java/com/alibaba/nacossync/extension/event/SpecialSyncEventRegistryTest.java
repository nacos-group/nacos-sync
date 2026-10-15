package com.alibaba.nacossync.extension.event;

import com.alibaba.nacossync.pojo.model.TaskDO;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A delete unsubscribes the task and then removes the instances it synced. A poll of that task must not register them
 * again afterwards.
 */
class SpecialSyncEventRegistryTest {

    private final SpecialSyncEventRegistry specialSyncEventRegistry = new SpecialSyncEventRegistry();

    private final TaskDO taskDO = task();

    @Test
    void pollAfterUnsubscribeDoesNothing() {
        // The timer took the poll from the registry just before the delete unsubscribed the task.
        AtomicInteger polls = new AtomicInteger();
        specialSyncEventRegistry.subscribe(taskDO, task -> polls.incrementAndGet());
        SpecialSyncEvent poll = specialSyncEventRegistry.getAllSpecialSyncEvent().iterator().next();

        specialSyncEventRegistry.unsubscribe(taskDO);
        poll.sync();

        assertEquals(0, polls.get());
    }

    @Test
    void unsubscribeWaitsForARunningPoll() throws Exception {
        // E2E: a poll of ELOAD-5 registered its instances while the delete was removing them, so they stayed.
        CountDownLatch polling = new CountDownLatch(1);
        CountDownLatch pollMayFinish = new CountDownLatch(1);
        specialSyncEventRegistry.subscribe(taskDO, task -> {
            polling.countDown();
            await(pollMayFinish);
        });
        SpecialSyncEvent poll = specialSyncEventRegistry.getAllSpecialSyncEvent().iterator().next();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> running = executor.submit(() -> poll.sync());
            assertTrue(polling.await(5, TimeUnit.SECONDS));
            AtomicReference<Thread> unsubscriber = new AtomicReference<>();
            Future<?> unsubscribe = executor.submit(() -> {
                unsubscriber.set(Thread.currentThread());
                specialSyncEventRegistry.unsubscribe(taskDO);
            });
            awaitBlocked(unsubscriber);
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

    /** Waits until the thread is blocked on a monitor, as unsubscribe is while the poll runs. */
    private static void awaitBlocked(AtomicReference<Thread> thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.get() == null || thread.get().getState() != Thread.State.BLOCKED) {
            assertTrue(System.nanoTime() < deadline, "unsubscribe did not wait for the running poll");
            Thread.sleep(10);
        }
    }
}
