package com.alibaba.nacossync.extension.sharding;

import java.util.List;

/**
 * Created by maj on 2020/10/27.
 */
public interface ServersManager<T> {

    public List<String> getServers() throws Exception;

    public void subscribeServers(T listener) throws Exception;

    public void register(String ip, int port) throws Exception;


}
