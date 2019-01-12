package com.alibaba.nacossync.extension.event;

import com.alibaba.nacossync.pojo.model.TaskDO;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * @author paderlol
 * @date: 2019-01-12 22:42
 */
@Service
public class SpecialSyncEventBus {
    private ConcurrentHashMap<String, SpecialSyncEvent> specialSyncEventRegistry = new ConcurrentHashMap<>();

    public void subscribe(TaskDO taskDO, Consumer<TaskDO> syncAction) {
        SpecialSyncEvent specialSyncEvent = new SpecialSyncEvent();
        specialSyncEvent.setTaskDO(taskDO);
        specialSyncEvent.setSyncAction(syncAction);
        specialSyncEventRegistry.putIfAbsent(taskDO.getTaskId(), specialSyncEvent);
    }

    public void unsubscribe(TaskDO taskDO) {
        specialSyncEventRegistry.remove(taskDO.getTaskId());
    }

    public Collection<SpecialSyncEvent> getAllSpecialSyncEvent() {
        return specialSyncEventRegistry.values();
    }
}
