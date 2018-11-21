package com.alibaba.nacossync.extension;

import com.alibaba.nacossync.pojo.model.TaskDO;

public interface SyncService {

    public boolean delete(TaskDO taskDO);

    public boolean sync(TaskDO taskDO);

}
