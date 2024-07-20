/**
 * Alipay.com Inc. Copyright (c) 2004-2019 All Rights Reserved.
 */
package com.alibaba.nacossync.monitor;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.dao.TaskAccessService;
import io.micrometer.core.instrument.Metrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * @author NacosSync
 * @version $Id: MetricsManager.java, v 0.1 2019年02月25日 上午1:13 NacosSync Exp $
 */
@Slf4j
@Service
public class MetricsManager implements CommandLineRunner {

    private final SkyWalkerCacheServices skyWalkerCacheServices;

    private final ClusterAccessService clusterAccessService;

    private final TaskAccessService taskAccessService;
    
    public MetricsManager(SkyWalkerCacheServices skyWalkerCacheServices, ClusterAccessService clusterAccessService,
            TaskAccessService taskAccessService) {
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.clusterAccessService = clusterAccessService;
        this.taskAccessService = taskAccessService;
    }
    
    /**
     * Callback used to run the bean.
     *
     * @param args incoming main method arguments
     * @throws Exception on error
     */
    @Override
    public void run(String... args) throws Exception {

        Metrics.gauge(MetricsStatisticsType.CACHE_SIZE.getMetricsName(), this,
                MetricsManager::getCacheSize
        );
        Metrics.gauge(MetricsStatisticsType.CLUSTER_SIZE.getMetricsName(), this,
                MetricsManager::getClusterSize
        );
        Metrics.gauge(MetricsStatisticsType.TASK_SIZE.getMetricsName(), this,
                MetricsManager::getTaskSize
        );
    }

    private Long getClusterSize() {
        return clusterAccessService.findPageNoCriteria(1, Integer.MAX_VALUE).getTotalElements();
    }

    private Long getTaskSize() {
        return taskAccessService.findPageNoCriteria(1, Integer.MAX_VALUE).getTotalElements();
    }

    private Integer getCacheSize() {
        return skyWalkerCacheServices.getFinishedTaskMap().size();
    }

    public void record(MetricsStatisticsType metricsStatisticsType, long amount) {

        Metrics.timer(metricsStatisticsType.getMetricsName()).record(amount, TimeUnit.MILLISECONDS);
    }

    public void recordError(MetricsStatisticsType metricsStatisticsType) {
        Metrics.counter(metricsStatisticsType.getMetricsName()).increment();
    }

}