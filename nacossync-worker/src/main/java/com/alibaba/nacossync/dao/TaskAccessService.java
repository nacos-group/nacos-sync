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
package com.alibaba.nacossync.dao;

import com.alibaba.nacossync.pojo.QueryCondition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import com.alibaba.nacossync.dao.repository.TaskRepository;
import com.alibaba.nacossync.pojo.model.TaskDO;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;

/**
 * @author NacosSync
 * @version $Id: TaskAccessService.java, v 0.1 2018-09-25 AM12:07 NacosSync Exp $$
 */
@Service
public class TaskAccessService implements PageQueryService<TaskDO> {

    @Autowired
    private TaskRepository taskRepository;

    public TaskDO findByTaskId(String taskId) {

        return taskRepository.findByTaskId(taskId);
    }

    public void deleteTaskById(String taskId) {
        taskRepository.deleteByTaskId(taskId);
    }
    
    /**
     * batch delete tasks by taskIds
     * @author yongchao9
     * @param taskIds
     */
    public void deleteTaskInBatch(List<String> taskIds) {
    	List<TaskDO> tds=taskRepository.findAllByTaskIdIn(taskIds);
        taskRepository.deleteInBatch(tds);
    }

    public Iterable<TaskDO> findAll() {

        return taskRepository.findAll();
    }

    public void addTask(TaskDO taskDO) {

        taskRepository.save(taskDO);

    }

    private Predicate getPredicate(CriteriaBuilder criteriaBuilder, List<Predicate> predicates) {
        Predicate[] p = new Predicate[predicates.size()];
        return criteriaBuilder.and(predicates.toArray(p));
    }

    private List<Predicate> getPredicates(Root<TaskDO> root, CriteriaBuilder criteriaBuilder, QueryCondition queryCondition) {

        List<Predicate> predicates = new ArrayList<>();
        predicates.add(criteriaBuilder.like(root.get("serviceName"), "%" + queryCondition.getServiceName() + "%"));

        return predicates;
    }

    @Override
    public Page<TaskDO> findPageNoCriteria(Integer pageNum, Integer size) {

        Pageable pageable = PageRequest.of(pageNum, size, Sort.Direction.DESC, "id");

        return taskRepository.findAll(pageable);
    }

    @Override
    public Page<TaskDO> findPageCriteria(Integer pageNum, Integer size, QueryCondition queryCondition) {

        Pageable pageable = PageRequest.of(pageNum, size, Sort.Direction.DESC, "id");

        return getTaskDOS(queryCondition, pageable);
    }

    private Page<TaskDO> getTaskDOS(QueryCondition queryCondition, Pageable pageable) {
        return taskRepository.findAll(
                (Specification<TaskDO>) (root, criteriaQuery, criteriaBuilder) -> {

                    List<Predicate> predicates = getPredicates(root,
                            criteriaBuilder, queryCondition);

                    return getPredicate(criteriaBuilder, predicates);

                }, pageable);
    }

}
