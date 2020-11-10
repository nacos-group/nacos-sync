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
package com.alibaba.nacossync.dao.repository;

import java.util.List;

import javax.transaction.Transactional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.CrudRepository;

import com.alibaba.nacossync.pojo.model.TaskDO;

/**
 * @author NacosSync
 * @version $Id: TaskRepository.java, v 0.1 2018-09-25 AM12:04 NacosSync Exp $$
 */
public interface TaskRepository extends CrudRepository<TaskDO, Integer>, JpaRepository<TaskDO, Integer>,
        JpaSpecificationExecutor<TaskDO> {

    TaskDO findByTaskId(String taskId);

    @Transactional
    int deleteByTaskId(String taskId);
    
    List<TaskDO> findAllByTaskIdIn(List<String> taskIds);
    
    List<TaskDO> getAllByWorkerIp(String workerIp);

}
