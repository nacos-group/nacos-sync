package com.alibaba.nacossync.extension.client;

import com.alibaba.nacossync.pojo.view.TaskModel;
import java.util.List;

public interface SyncQueryClient {


    List<TaskModel> getAllInstance(InstanceQueryModel instanceQueryModel);

}
