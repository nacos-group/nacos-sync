/**
 * Alipay.com Inc. Copyright (c) 2004-2019 All Rights Reserved.
 */
package com.alibaba.nacossync.monitor;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.dao.ClusterAccessService;
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

    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;

    @Autowired
    private ClusterAccessService clusterAccessService;

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

    private Integer getClusterSize() {
        return clusterAccessService.findPageNoCriteria(1, Integer.MAX_VALUE).getNumber();
    }

    private Integer getTaskSize() {
        return clusterAccessService.findPageNoCriteria(1, Integer.MAX_VALUE).getNumber();
    }

    private Integer getCacheSize() {
        return skyWalkerCacheServices.getFinishedTaskMap().size();
    }

    public void record(MetricsStatisticsType metricsStatisticsType, long amount) {

        Metrics.timer(metricsStatisticsType.getMetricsName()).record(amount, TimeUnit.MILLISECONDS);
    }

}