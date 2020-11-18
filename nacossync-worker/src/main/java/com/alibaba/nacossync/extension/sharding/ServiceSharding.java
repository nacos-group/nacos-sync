package com.alibaba.nacossync.extension.sharding;

import com.alibaba.nacossync.extension.impl.extend.Sharding;
import com.alibaba.nacossync.pojo.ShardingLog;

import java.util.List;
import java.util.Queue;
import java.util.TreeSet;

/**
 * Created by maj on 2020/10/27.
 */
public interface ServiceSharding {

    public void sharding(String key, List<String> serviceNames);

    public TreeSet<String> getLocalServices(String key);

    public boolean addServerChange(String name, Sharding sharding);

    public Queue<ShardingLog> getChangeServices(String key);
}
