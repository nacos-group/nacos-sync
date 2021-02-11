package com.alibaba.nacossync.extension.sharding;

import com.alibaba.nacossync.extension.impl.extend.Sharding;
import com.alibaba.nacossync.pojo.ShardingLog;
import com.alibaba.nacossync.pojo.model.TaskDO;

import java.util.List;
import java.util.Queue;
import java.util.TreeSet;

/**
 * Created by maj on 2020/10/27.
 */
public interface ServiceSharding {

    void sharding(String key, List<String> serviceNames);

    TreeSet<String> getLocalServices(String key);

    boolean addServerChange(String name, Sharding sharding);

    Queue<ShardingLog> getChangeServices(String key);

    boolean isProcess(TaskDO taskDO, String serviceName);

    void shardingWithOutAddChange(String key, List<String> serviceNames);
}
