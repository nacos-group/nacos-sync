package com.alibaba.nacossync.extension.event.listener;

import com.alibaba.nacossync.extension.event.SpecialSyncEvent;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.function.Consumer;

/**
 * @author paderlol
 * @date: 2019-01-12 22:29
 */
@Service
@Slf4j
public class SpecialSyncEventListener {
    @Autowired
    private EventBus eventBus;

    @PostConstruct
    public void init() {
        eventBus.register(this);
    }

    @Subscribe
    public void listenerSpecialSyncEvent(SpecialSyncEvent specialSyncEvent) {
        TaskDO taskDO = specialSyncEvent.getTaskDO();
        Consumer<TaskDO> syncAction = specialSyncEvent.getSyncAction();
        syncAction.accept(taskDO);
    }

}
