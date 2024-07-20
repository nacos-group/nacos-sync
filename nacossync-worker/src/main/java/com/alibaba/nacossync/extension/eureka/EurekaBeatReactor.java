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
package com.alibaba.nacossync.extension.eureka;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * @author liu jun jie
 * @date 2019-06-26
 */
@Slf4j
public class EurekaBeatReactor {
    private final ScheduledExecutorService executorService;

    private static final long CLIENT_BEAT_INTERVAL = 5 * 1000;
    private final Map<String, InstanceInfo> eurekaBeat = new ConcurrentHashMap<>();
    private final EurekaHttpClient eurekaHttpClient;

    public EurekaBeatReactor(EurekaHttpClient eurekaHttpClient) {
        this.eurekaHttpClient = eurekaHttpClient;
        executorService = new ScheduledThreadPoolExecutor(30, r -> {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("com.alibaba.nacossync.eureka.beat.sender");
            return thread;
        });

        executorService.schedule(new BeatProcessor(), 0, TimeUnit.SECONDS);
    }

    public void addInstance(String key, InstanceInfo value) {
        this.eurekaBeat.put(key, value);
    }

    public void removeInstance(String key) {
        log.debug("[BEAT] removing beat: {} to beat map.", key);
        this.eurekaBeat.remove(key);
    }

    class BeatProcessor implements Runnable {

        @Override
        public void run() {
            try {
                for (Map.Entry<String, InstanceInfo> entry : eurekaBeat.entrySet()) {
                    InstanceInfo instanceInfo = entry.getValue();
                    executorService.schedule(() -> {
                        log.debug("[BEAT] adding beat: {} to beat map.", instanceInfo.getId());
                        eurekaHttpClient.sendHeartBeat(instanceInfo.getAppName(), instanceInfo.getId(), instanceInfo, InstanceInfo.InstanceStatus.UP);
                    }, 0, TimeUnit.MILLISECONDS);
                }
            } catch (Exception e) {
                log.error("[CLIENT-BEAT] Exception while scheduling beat.", e);
            } finally {
                executorService.schedule(this, CLIENT_BEAT_INTERVAL, TimeUnit.MILLISECONDS);
            }
        }
    }
}



