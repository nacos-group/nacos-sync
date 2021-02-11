package com.alibaba.nacossync.extension.impl.extend;

import com.alibaba.nacossync.pojo.ShardingLog;
import com.alibaba.nacossync.pojo.model.TaskDO;

import java.util.List;
import java.util.Queue;
import java.util.TreeSet;

/**
 * Created by maj on 2020/10/30.
 */
public interface Sharding {

    void onServerChange();

    void start(TaskDO taskDO);

    void stop(TaskDO taskDO);

    void doSharding(String key, List<String> serviceNames);

    TreeSet<String> getLocalServices();

    Queue<ShardingLog> getChangeService();

    void reShardingIfNeed();

    boolean isProcess(TaskDO taskDO, String serviceName);
}
