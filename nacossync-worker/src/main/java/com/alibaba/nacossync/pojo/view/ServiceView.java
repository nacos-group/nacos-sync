package com.alibaba.nacossync.pojo.view;

import lombok.Data;

/**
 * @author wyd
 * @since 2024-01-04 15:54:36
 */
@Data
public class ServiceView {
    private String name;

    private String groupName;

    private int clusterCount;

    private int ipCount;

    private int healthyInstanceCount;

    private String triggerFlag;
}
