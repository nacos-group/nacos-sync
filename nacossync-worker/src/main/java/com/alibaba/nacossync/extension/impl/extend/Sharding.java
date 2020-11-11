package com.alibaba.nacossync.extension.impl.extend;

import com.alibaba.nacossync.pojo.model.TaskDO;

import java.util.List;
import java.util.Queue;
import java.util.TreeSet;

/**
 * Created by maj on 2020/10/30.
 */
public interface Sharding {

    public void onServerChange();

    public void start(TaskDO taskDO);

    public void stop(TaskDO taskDO);

    public void doSharding(String key, List<String> serviceNames);

    public Queue<String> getaAddServices();

    public Queue<String> getRemoveServices();

    public TreeSet<String> getLocalServices(String key);
}
