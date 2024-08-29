/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacossync.pojo.view;

import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import lombok.Data;

import java.io.Serializable;

/**
 * @author NacosSync
 * @version $Id: ClusterModel.java, v 0.1 2018-09-25 下午11:09 NacosSync Exp $$
 */
@Data
public class ClusterModel implements Serializable {
    
    private String clusterId;
    
    /**
     * json format，["192.168.1:8080","192.168.2?key=1"]
     */
    private String connectKeyList;
    
    /**
     * cluster name, eg：cluster of ShangHai（edas-sh）
     */
    private String clusterName;
    
    /**
     * cluster type, eg cluster of CS,cluster of Nacos,
     *
     * @see ClusterTypeEnum
     */
    private String clusterType;
    
    private String namespace;
    
    private String userName;
    
    public static ClusterModel from(ClusterDO clusterDO) {
        ClusterModel clusterModel = new ClusterModel();
        clusterModel.setClusterId(clusterDO.getClusterId());
        clusterModel.setConnectKeyList(clusterDO.getConnectKeyList());
        clusterModel.setClusterType(clusterDO.getClusterType());
        clusterModel.setClusterName(clusterDO.getClusterName());
        clusterModel.setNamespace(clusterDO.getNamespace());
        clusterModel.setUserName(clusterDO.getUserName());
        return clusterModel;
    }
}
