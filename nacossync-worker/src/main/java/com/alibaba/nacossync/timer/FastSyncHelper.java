/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacossync.timer;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.alibaba.nacossync.extension.impl.NacosSyncToNacosServiceImpl;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static com.alibaba.nacossync.constant.SkyWalkerConstants.MAX_THREAD_NUM;

/**
 * multi-threaded synchronization Task DO.
 *
 * @ClassName: FastSyncHelper
 * @Author: ChenHao26
 * @Date: 2022/7/19 17:02
 * @Description:
 */
@Service
@Slf4j
public class FastSyncHelper {
    
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    
    private final MetricsManager metricsManager;
    
    private final SyncManagerService syncManagerService;
    
    private final NacosSyncToNacosServiceImpl nacosSyncToNacosService;
    
    private final ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREAD_NUM);
    
    public FastSyncHelper(SkyWalkerCacheServices skyWalkerCacheServices, MetricsManager metricsManager,
            SyncManagerService syncManagerService, NacosSyncToNacosServiceImpl nacosSyncToNacosService) {
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.metricsManager = metricsManager;
        this.syncManagerService = syncManagerService;
        this.nacosSyncToNacosService = nacosSyncToNacosService;
    }
    
    
    /**
     * every 200 services start a thread to perform synchronization.
     *
     * @param taskDOS task list
     */
    public void syncWithThread(List<TaskDO> taskDOS) {
        sync(taskDOS, tuple -> {
            for (TaskDO task : tuple.getT2()) {
                //执行兜底的定时同步
                nacosSyncToNacosService.timeSync(task);
            }
        });
    }
    
    
    /**
     * every 200 services start a thread to perform synchronization.
     *
     * @param taskDO         task info
     * @param filterServices filterServices
     */
    public void syncWithThread(TaskDO taskDO, List<String> filterServices) {
        sync(filterServices, tuple -> {
            //  执行数据同步
            for (String serviceName : tuple.getT2()) {
                syncByIndex(taskDO, serviceName, tuple.getT1());
            }
        });
    }
    
    public <T> void sync(List<T> items, Consumer<Tuple<Integer, List<T>>> itemConsumer) {
        long startTime = System.currentTimeMillis();
        List<Tuple<Integer, List<T>>> taskGroupList = averageAssign(items, MAX_THREAD_NUM);
        
        // 等待所有任务完成
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(taskGroupList.stream()
                .map(tuple -> CompletableFuture.runAsync(() -> performSync(tuple, itemConsumer), executorService))
                .toArray(CompletableFuture[]::new));
        try {
            allTasks.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        log.info("新增同步任务数量 {}, 执行耗时：{}ms", items.size(), System.currentTimeMillis() - startTime);
    }
    
    private <T> void performSync(Tuple<Integer, List<T>> tuple, Consumer<Tuple<Integer, List<T>>> itemConsumer) {
        if (tuple == null || tuple.getT2() == null || tuple.getT2().isEmpty()) {
            return;
        }
        itemConsumer.accept(tuple);
        
    }
    

    
    private void syncByIndex(TaskDO taskDO, String serviceName, int index) {
        long startTime = System.currentTimeMillis();
        TaskDO task = new TaskDO();
        BeanUtils.copyProperties(taskDO, task);
        task.setServiceName(serviceName);
        task.setOperationId(taskDO.getTaskId() + serviceName);
        if (syncManagerService.sync(task, index)) {
            skyWalkerCacheServices.addFinishedTask(task);
            log.info("sync thread : {} sync finish ,time consuming ：{}", Thread.currentThread().getId(),
                    System.currentTimeMillis() - startTime);
            metricsManager.record(MetricsStatisticsType.SYNC_TASK_RT, System.currentTimeMillis() - startTime);
        } else {
            log.warn("listenerSyncTaskEvent sync failure.");
        }
    }
    
    /**
     * 将一个List均分成n个list,主要通过偏移量来实现的
     *
     * @param source 源集合
     * @param limit  最大值
     * @return
     */
    public static <T> List<Tuple<Integer, List<T>>> averageAssign(List<T> source, int limit) {
        if (null == source || source.isEmpty()) {
            return Collections.emptyList();
        }
        int size = source.size();
        List<Tuple<Integer, List<T>>> result = new ArrayList<>();
        // 通过减去1并加1，我们可以确保将多余的元素放在最后一个子列表中。在上述示例中，计算结果为 ((10 - 1) / 3 + 1) = 4，我们创建了4个子列表，其中最后一个子列表包含2个元素，而不是1个。这样可以更均匀地分配源列表的元素.
        int listCount = (int) Math.ceil((double) source.size() / limit);  // 计算子列表数量，使用 Math.ceil 向上取整，确保多余的元素放在最后一个子列表中
        int remainder = source.size() % listCount;  // 计算多余的元素数量
        int assigned = 0;  // 记录已分配的元素索引
        for (int i = 0; i < listCount; i++) {
            int sublistSize = size / listCount + (remainder-- > 0 ? 1 : 0);  // 计算子列表大小，平均分配元素，并在有多余元素时将其分配到子列表中
            List<T> sublist = source.subList(assigned, assigned + sublistSize);  // 获取子列表
            result.add(Tuple.of(i, sublist));  // 将子列表添加到结果列表
            assigned += sublistSize;  // 更新已分配的元素索引
        }
        
        return result;
    }
    
    
}
