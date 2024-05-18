package com.alibaba.nacossync.event;

import com.alibaba.nacossync.pojo.model.TaskDO;
import lombok.Data;

@Data
public class DeleteAllSubTaskEvent {
    public DeleteAllSubTaskEvent(TaskDO taskDO) {
        this.taskDO = taskDO;
    }
    
    private final TaskDO taskDO;
}
