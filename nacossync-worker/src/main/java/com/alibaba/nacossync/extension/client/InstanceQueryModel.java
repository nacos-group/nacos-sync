package com.alibaba.nacossync.extension.client;

import lombok.Data;

@Data
public class InstanceQueryModel {

    private String sourceClusterId;
    private String destClusterId;
    private String serviceName;
    private String groupName;
    private String version;
    private int pageNo;
    private int pageSize;
}
