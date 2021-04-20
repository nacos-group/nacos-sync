package com.alibaba.nacossync.pojo;

/**
 * Created by maj on 2020/11/18.
 */
public class ShardingLog {

    private String serviceName;

    private String type;

    public ShardingLog() {
    }

    public ShardingLog(String serviceName, String type) {
        this.serviceName = serviceName;
        this.type = type;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
