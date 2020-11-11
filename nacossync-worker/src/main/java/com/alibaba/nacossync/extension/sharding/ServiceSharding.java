package com.alibaba.nacossync.extension.sharding;

import com.alibaba.nacossync.extension.impl.extend.Sharding;

import java.util.List;
import java.util.Queue;
import java.util.TreeSet;

/**
 * Created by maj on 2020/10/27.
 */
public interface ServiceSharding {

    public void sharding(String key, List<String> serviceNames);

    public Queue<String> getaAddServices(String key);

    public Queue<String> getRemoveServices(String key);

    public TreeSet<String> getLoacalServices(String key);

    public boolean addServerChange(String name, Sharding sharding);
}
