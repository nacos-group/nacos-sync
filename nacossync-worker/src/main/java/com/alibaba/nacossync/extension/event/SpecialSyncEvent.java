package com.alibaba.nacossync.extension.event;

import com.alibaba.nacossync.pojo.model.TaskDO;
import lombok.Data;

import java.util.function.Consumer;

/**
 * @author paderlol
 * @date: 2019-01-12 22:28
 */
@Data
public class SpecialSyncEvent {
    private TaskDO taskDO;
    private Consumer<TaskDO> syncAction;
}
