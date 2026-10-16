/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.alibaba.nacossync.util;

import com.google.common.util.concurrent.Striped;

import java.util.concurrent.locks.Lock;

/**
 * Keeps starting a task and deleting it apart. A delete removes the task row and stops the task under the task's
 * lock, and the sync timer re-reads the row under the same lock before it starts the task. Without it the timer can
 * start a task from a row it read just before the delete, after the delete has stopped it, and the deleted task keeps
 * running.
 * <p>
 * The timers only try the lock and skip a task they cannot lock: they go through all tasks on one thread, and a task
 * that is being deleted, or that shares a lock stripe with a slow one, would hold up every task after it. The next
 * round comes a few seconds later.
 */
public final class TaskLocks {

    /**
     * An ALL task holds its lock until all its new sub-tasks have started, which can take a while for a big source.
     * Tasks sharing its stripe are skipped by the timers, and their deletes wait, all that time: keep them few.
     */
    private static final Striped<Lock> LOCKS = Striped.lock(1024);

    private TaskLocks() {
    }

    /**
     * Runs the action under the task's lock if it is free, and returns whether it ran.
     */
    public static boolean tryRunLocked(String taskId, Runnable action) {
        Lock lock = LOCKS.get(taskId);
        if (!lock.tryLock()) {
            return false;
        }
        try {
            action.run();
        } finally {
            lock.unlock();
        }
        return true;
    }

    public static void runLocked(String taskId, Runnable action) {
        Lock lock = LOCKS.get(taskId);
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }
}
