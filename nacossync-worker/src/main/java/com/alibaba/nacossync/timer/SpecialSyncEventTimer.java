package com.alibaba.nacossync.timer;

import com.alibaba.nacossync.constant.TaskStatusEnum;
import com.alibaba.nacossync.extension.event.SpecialSyncEvent;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.google.common.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author paderlol
 * @date: 2019-01-12 22:53
 */
@Slf4j
@Service
public class SpecialSyncEventTimer implements CommandLineRunner {

    @Autowired
    private SpecialSyncEventBus specialSyncEventBus;

    @Autowired
    private EventBus eventBus;
    @Autowired
    private ScheduledExecutorService scheduledExecutorService;

    @Override
    public void run(String... args) throws Exception {
        /** 3s去数据库捞一次任务列表 */
        scheduledExecutorService.scheduleWithFixedDelay(new SpecialSyncEventTimer.SpecialSyncEventThread(), 0, 3000,
            TimeUnit.MILLISECONDS);
    }

    private class SpecialSyncEventThread implements Runnable {

        @Override
        public void run() {
            try {
                Collection<SpecialSyncEvent> allSpecialSyncEvent = specialSyncEventBus.getAllSpecialSyncEvent();
                allSpecialSyncEvent.stream()
                    .filter(specialSyncEvent -> TaskStatusEnum.SYNC.getCode()
                        .equals(specialSyncEvent.getTaskDO().getTaskStatus()))
                    .forEach(specialSyncEvent -> eventBus.post(specialSyncEvent));
            } catch (Exception e) {
                log.warn("SpecialSyncEventThread Exception", e);

            }
        }
    }
}
