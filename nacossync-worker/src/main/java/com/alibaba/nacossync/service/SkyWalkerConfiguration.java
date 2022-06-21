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
package com.alibaba.nacossync.service;

import java.util.concurrent.*;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.eventbus.EventBus;

/**
 * @author NacosSync
 * @version $Id: SkyWalkerConfiguration.java, v 0.1 2018-09-27 AM1:20 NacosSync Exp $$
 */
@Configuration
public class SkyWalkerConfiguration {

    @Bean
    public EventBus eventBus() {
        return new EventBus();
    }

    @Bean
    public ScheduledExecutorService executorService() {

        return new ScheduledThreadPoolExecutor(5, new BasicThreadFactory.Builder()
                .namingPattern("SkyWalker-Timer-schedule-pool-%d").daemon(true).build());
    }

    @Bean
    public ThreadPoolExecutor threadPoolExecutor() {
        int corePollSize = Runtime.getRuntime().availableProcessors() + 1;

        int maxPollSize = 8192;
        BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<>(maxPollSize);
        RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        ThreadFactory threadFactory = r -> {
            Thread thread = new Thread(r);
            thread.setName("nacos-sync-pool" + thread.getId());
            return thread;
        };
        return new ThreadPoolExecutor(corePollSize, maxPollSize, 0, TimeUnit.MILLISECONDS,
                blockingQueue, threadFactory, rejectedExecutionHandler);
    }

}
