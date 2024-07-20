/**
 * Alipay.com Inc. Copyright (c) 2004-2019 All Rights Reserved.
 */
package com.alibaba.nacossync.constant;

import lombok.Getter;

/**
 * @author NacosSync
 * @version $Id: MetricsStatisticsType.java, v 0.1 2019年02月28日 下午2:17 NacosSync Exp $
 */
@Getter
public enum MetricsStatisticsType {

    CACHE_SIZE("nacosSync.finished.taskMap.cacheSize", "任务执行完成缓存列表数"),

    TASK_SIZE("nacosSync.task.size", "同步任务数"),

    CLUSTER_SIZE("nacosSync.cluster.size", "集群数"),

    SYNC_TASK_RT("nacosSync.add.task.rt", "同步任务执行耗时"),

    DELETE_TASK_RT("nacosSync.delete.task.rt", "删除任务耗时"),

    DISPATCHER_TASK("nacosSync.dispatcher.task", "从数据库中分发任务"),

    SYNC_ERROR("nacosSync.sync.task.error", "所有同步执行时的异常"),

    DELETE_ERROR("nacosSync.delete.task.error", "所有删除同步执行时的异常");

    /**
     * metricsName
     */
    private final String metricsName;

    MetricsStatisticsType(String code, String desc) {
        this.metricsName = code;
    }
    
}