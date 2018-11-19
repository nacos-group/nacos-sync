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

import java.io.Serializable;

import lombok.Data;

/**
 * @author NacosSync
 * @version $Id: ClusterModel.java, v 0.1 2018-09-25 下午11:09 NacosSync Exp $$
 */
@Data
public class ClusterModel implements Serializable {

    private String clusterId;
    /** json格式，["192.168.1:8080","192.168.2?key=1"] */
    private String connectKeyList;
    /** 集群名字，用于展示，例如：上海集群（edas-sh） */
    private String clusterName;
    /** 集群类型，例如是CS集群，Nacos集群，见ClusterType */
    private String clusterType;
}
