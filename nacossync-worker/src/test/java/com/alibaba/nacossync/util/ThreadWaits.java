package com.alibaba.nacossync.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lets a test wait for a thread that should block, instead of sleeping and hoping it got there.
 */
public final class ThreadWaits {

    private ThreadWaits() {
    }

    /**
     * Waits until the thread waits right in the given method, i.e. on a monitor or a lock it takes there.
     */
    public static void awaitWaitingIn(AtomicReference<Thread> thread, Class<?> type, String method, String message)
            throws InterruptedException {
        awaitUntil(() -> isWaitingIn(thread.get(), type, method), message);
    }

    public static void awaitUntil(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, message);
            Thread.sleep(10);
        }
    }

    /**
     * Whether the thread waits in the given method with only JDK frames above it, as on a monitor or a lock taken
     * there. A thread waiting further in, e.g. in a test's answer called from the method, does not count.
     */
    public static boolean isWaitingIn(Thread thread, Class<?> type, String method) {
        if (thread == null) {
            return false;
        }
        Thread.State state = thread.getState();
        if (state != Thread.State.BLOCKED && state != Thread.State.WAITING && state != Thread.State.TIMED_WAITING) {
            return false;
        }
        for (StackTraceElement frame : thread.getStackTrace()) {
            if (frame.getClassName().equals(type.getName()) && frame.getMethodName().equals(method)) {
                return true;
            }
            String name = frame.getClassName();
            if (!name.startsWith("java.") && !name.startsWith("jdk.") && !name.startsWith("sun.")) {
                return false;
            }
        }
        return false;
    }
}
