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
package com.alibaba.nacossync.pojo.model;

import com.alibaba.nacossync.constant.ClusterTypeEnum;
import java.io.Serializable;

import javax.persistence.*;

import lombok.Data;

/**
 * @author NacosSync
 * @version $Id: EnvDO.java, v 0.1 2018-09-25 PM 4:17 NacosSync Exp $$
 */
@Data
@Entity
@Table(name = "cluster")
public class ClusterDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    /**
     * custom cluster id(unique)
     */
    private String clusterId;
    /**
     * Linked list,When a connection is established, one is chosen at random, no fixed format and
     * each cluster is not the same,eg nacos＝["192.168.1:8080","192.168.2:8080"],C＝["192.168.1?key=1","192.168.2?key=1"]
     */
    private String connectKeyList;
    /**
     * cluster name use to display eg：cluster of ShangHai（edas-sh）
     */
    private String clusterName;
    /**
     * cluster type ，eg CS cluster , Nacos cluster，
     *
     * @see ClusterTypeEnum
     */
    private String clusterType;

    /**
     * The username of the Nacos.
     *
     */
    private String userName;

    /**
     * The password of the Nacos.
     */
    private String password;

}
