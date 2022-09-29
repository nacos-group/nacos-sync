/**
 * Alipay.com Inc. Copyright (c) 2004-2019 All Rights Reserved.
 */
package com.alibaba.nacossync.constant;

/**
 * @author NacosSync
 * @version $Id: MetricsStatisticsType.java, v 0.1 2019/02/28 2:17 NacosSync Exp $
 */
public enum MetricsStatisticsType {

    CACHE_SIZE("nacosSync.finished.taskMap.cacheSize", "The number of task execution completed cache lists"),

    TASK_SIZE("nacosSync.task.size", "Number of sync tasks"),

    CLUSTER_SIZE("nacosSync.cluster.size", "Number of clusters"),

    SYNC_TASK_RT("nacosSync.add.task.rt", "Time-consuming synchronization task execution"),

    DELETE_TASK_RT("nacosSync.delete.task.rt", "Time consuming to delete tasks"),

    DISPATCHER_TASK("nacosSync.dispatcher.task", "Distribute tasks from the database"),

    SYNC_ERROR("nacosSync.sync.task.error", "Exceptions on all synchronous executions"),

    DELETE_ERROR("nacosSync.delete.task.error", "Exception when all deletes are executed synchronously");

    /**
     * metricsName
     */
    private String metricsName;
    private String desc;

    MetricsStatisticsType(String code, String desc) {
        this.metricsName = code;
        this.desc = desc;
    }

    public String getMetricsName() {
        return metricsName;
    }
}