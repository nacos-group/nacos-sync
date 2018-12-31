/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.alibaba.nacossync.constant;

import java.util.ArrayList;
import java.util.List;

/**
 * @author NacosSync
 * @version $Id: ClusterTypeEnum.java, v 0.1 2018-09-25 下午4:38 NacosSync Exp $$
 */
public enum ClusterTypeEnum {

    CS("CS", "configserver集群"),

    NACOS("NACOS", "nacos集群"), EUREKA("EUREKA", "eureka集群"),

    ZK("ZK", "zookeeper集群");

    // CONSUL("CONSUL", "consul集群");

    private String code;

    private String desc;

    ClusterTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getClusterTypeCodes() {

        List<String> list = new ArrayList<String>();

        for (ClusterTypeEnum clusterTypeEnum : ClusterTypeEnum.values()) {
            list.add(clusterTypeEnum.getCode());
        }
        return list;
    }

    /**
     * Getter method for property <tt>code</tt>.
     *
     * @return property value of code
     */
    public String getCode() {
        return code;
    }

    /**
     * Setter method for property <tt>code </tt>.
     *
     * @param code value to be assigned to property code
     */
    public void setCode(String code) {
        this.code = code;
    }

    /**
     * Getter method for property <tt>desc</tt>.
     *
     * @return property value of desc
     */
    public String getDesc() {
        return desc;
    }

    /**
     * Setter method for property <tt>desc </tt>.
     *
     * @param desc value to be assigned to property desc
     */
    public void setDesc(String desc) {
        this.desc = desc;
    }

    public static boolean contains(String clusterType) {

        for (ClusterTypeEnum clusterTypeEnum : ClusterTypeEnum.values()) {
            if (clusterTypeEnum.getCode().equals(clusterType)) {
                return true;
            }

        }
        return false;
    }
}
