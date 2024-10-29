package com.alibaba.nacossync.utils;

import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.BatchTaskExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchTaskExecutorTest {

    /**
     * Test the batch operation to ensure all tasks are executed.
     */
    @Test
    public void testBatchOperation() {
        // Prepare a list of tasks
        List<TaskDO> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TaskDO task = new TaskDO();
            task.setServiceName("Service" + i);
            tasks.add(task);
        }

        // Create a counter to track the number of executed tasks
        AtomicInteger counter = new AtomicInteger(0);

        // Execute batch operation
        BatchTaskExecutor.batchOperation(tasks, task -> counter.incrementAndGet());

        // Verify that all tasks have been executed
        assertEquals(tasks.size(), counter.get());
    }

    /**
     * Test the averageAssign method to ensure the correct distribution of tasks.
     */


    /**
     * Test the batch operation with an empty list to ensure no errors occur.
     */
    @Test
    public void testBatchOperationWithEmptyList() {
        // Prepare an empty list of tasks
        List<TaskDO> tasks = new ArrayList<>();

        // Create a counter to track the number of executed tasks
        AtomicInteger counter = new AtomicInteger(0);

        // Execute batch operation
        BatchTaskExecutor.batchOperation(tasks, task -> counter.incrementAndGet());

        // Verify that no tasks have been executed
        assertEquals(0, counter.get());
    }
}
