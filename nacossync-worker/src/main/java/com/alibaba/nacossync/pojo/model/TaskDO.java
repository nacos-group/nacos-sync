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

import java.io.Serializable;

import javax.persistence.*;

import lombok.Data;

/**
 * @author NacosSync
 * @version $Id: TaskDo.java, v 0.1 2018-09-24 下午11:53 NacosSync Exp $$
 */
@Data
@Entity
@Table(name = "task", uniqueConstraints = {@UniqueConstraint(columnNames={"taskId"})})
public class TaskDO implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    /** 自定义生成的任务id，唯一 */
    private String  taskId;
    /** 源集群id */
    private String  sourceClusterId;
    /** 目标集群id */
    private String  destClusterId;
    /** 服务名 */
    private String  serviceName;
    /** 分组名 */
    private String  groupName;
    /** 当前任务状态 */
    private String  taskStatus;
    /** 执行这个任务的IP */
    private String  workerIp;
    /** 操作id,任务状态变更时，此id会跟着变更 */
    private String  operationId;
}
