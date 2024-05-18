package com.alibaba.nacossync.util;

import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
@Slf4j
public class BatchTaskExecutor {

    private static final int MAX_THREAD_NUM = 200;
    private static final ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREAD_NUM);

    /**
     * 批量操作方法
     *
     * @param items     任务列表
     * @param operation 要执行的操作
     */
    public static void batchOperation(List<TaskDO> items, Consumer<TaskDO> operation) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        List<Tuple<Integer, List<TaskDO>>> taskGroupList = averageAssign(items, MAX_THREAD_NUM);

        // 创建一个包含所有任务的 CompletableFuture
        CompletableFuture<?>[] futures = taskGroupList.stream().map(tuple -> CompletableFuture.runAsync(() -> {
            for (TaskDO taskDO : tuple.getT2()) {
                operation.accept(taskDO);
            }
        }, executorService)).toArray(CompletableFuture[]::new);

        try {
            // 等待所有任务完成
            CompletableFuture.allOf(futures).join();
        } catch (Exception e) {
            log.error("Error occurred during sync operation", e);
        }

        log.debug("Total sync tasks: {}, Execution time: {} ms", items.size(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    /**
     * 将一个List均分成n个list, 主要通过偏移量来实现的
     *
     * @param source 源集合
     * @param limit  最大值
     * @return 均分后的列表
     */
    private static <T> List<Tuple<Integer, List<T>>> averageAssign(List<T> source, int limit) {
        if (CollectionUtils.isEmpty(source)) {
            return Collections.emptyList();
        }

        int size = source.size();
        int listCount = (int) Math.ceil((double) size / limit);  // Calculate the number of sublist
        int remainder = size % listCount;  // Calculate the number of remaining elements after even distribution
        List<Tuple<Integer, List<T>>> result = new ArrayList<>(listCount);  // Initialize the result list with the expected size

        for (int i = 0, assigned = 0; i < listCount; i++) {
            int sublistSize = size / listCount + (remainder-- > 0 ? 1 : 0);  // Determine the size of each sublist, distribute remaining elements
            List<T> sublist = new ArrayList<>(source.subList(assigned, assigned + sublistSize));  // Create the sublist
            result.add(Tuple.of(i, sublist));  // Add the sublist to the result
            assigned += sublistSize;  // Update the assigned index
        }

        return result;
    }
}