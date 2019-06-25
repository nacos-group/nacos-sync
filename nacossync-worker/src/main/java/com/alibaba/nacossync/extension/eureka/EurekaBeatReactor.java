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
 * @author: Liu Jun Jie
 * @create: 2019-06-24 17:07
 **/
@Slf4j
public class EurekaBeatReactor {
    private ScheduledExecutorService executorService;

    private volatile long clientBeatInterval = 5 * 1000;
    private final Map<String, InstanceInfo> dom2Beat = new ConcurrentHashMap<String, InstanceInfo>();
    private EurekaHttpClient eurekaHttpClient;

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
        this.dom2Beat.put(key, value);
    }

    public void removeInstance(String key) {
        log.debug("[BEAT] removing beat: {} to beat map.", key);
        this.dom2Beat.remove(key);
    }

    class BeatProcessor implements Runnable {

        @Override
        public void run() {
            try {
                for (Map.Entry<String, InstanceInfo> entry : dom2Beat.entrySet()) {
                    InstanceInfo instanceInfo = entry.getValue();
                    executorService.schedule(() -> {
                        log.debug("[BEAT] adding beat: {} to beat map.", instanceInfo.getId());
                        eurekaHttpClient.sendHeartBeat(instanceInfo.getAppName(), instanceInfo.getId(), instanceInfo, InstanceInfo.InstanceStatus.UP);
                    }, 0, TimeUnit.MILLISECONDS);
                }
            } catch (Exception e) {
                log.error("[CLIENT-BEAT] Exception while scheduling beat.", e);
            } finally {
                executorService.schedule(this, clientBeatInterval, TimeUnit.MILLISECONDS);
            }
        }
    }
}



