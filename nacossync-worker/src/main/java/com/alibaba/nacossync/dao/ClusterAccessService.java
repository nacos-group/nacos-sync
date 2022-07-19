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
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;

import com.alibaba.nacossync.dao.repository.ClusterRepository;
import com.alibaba.nacossync.pojo.model.ClusterDO;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;

/**
 * @author NacosSync
 * @version $Id: ClusterAccessService.java, v 0.1 2018-09-25 PM9:32 NacosSync Exp $$
 */
@Service
@Slf4j
public class ClusterAccessService implements PageQueryService<ClusterDO> {

    @Autowired
    private ClusterRepository clusterRepository;

    public ClusterDO insert(ClusterDO clusterDO) {

        return clusterRepository.save(clusterDO);
    }

    public void deleteByClusterId(String clusterId) {

        clusterRepository.deleteByClusterId(clusterId);

    }

    public ClusterDO findByClusterId(String clusterId) {

        return clusterRepository.findByClusterId(clusterId);
    }

    @Override
    public Page<ClusterDO> findPageNoCriteria(Integer pageNum, Integer size) {

        Pageable pageable = PageRequest.of(pageNum, size, Sort.Direction.DESC, "id");

        return clusterRepository.findAll(pageable);
    }

    @Override
    public Page<ClusterDO> findPageCriteria(Integer pageNum, Integer size, QueryCondition queryCondition) {

        Pageable pageable = PageRequest.of(pageNum, size, Sort.Direction.DESC, "id");

        return getClusterDOS(queryCondition, pageable);
    }

    private Page<ClusterDO> getClusterDOS(QueryCondition queryCondition, Pageable pageable) {
        return clusterRepository.findAll(
                (Specification<ClusterDO>) (root, criteriaQuery, criteriaBuilder) -> {

                    List<Predicate> predicates = getPredicates(root,
                            criteriaBuilder, queryCondition);

                    return getPredicate(criteriaBuilder, predicates);

                }, pageable);
    }

    private Predicate getPredicate(CriteriaBuilder criteriaBuilder, List<Predicate> predicates) {

        Predicate[] p = new Predicate[predicates.size()];
        return criteriaBuilder.and(predicates.toArray(p));
    }

    private List<Predicate> getPredicates(Root<ClusterDO> root, CriteriaBuilder criteriaBuilder, QueryCondition queryCondition) {

        List<Predicate> predicates = new ArrayList<>();
        predicates.add(criteriaBuilder.like(root.get("clusterName"), "%" + queryCondition.getServiceName() + "%"));
        return predicates;
    }
    
    public int findClusterLevel(String sourceClusterId){
        ClusterDO clusterDO = clusterRepository.findByClusterId(sourceClusterId);
        if (clusterDO != null) {
            return clusterDO.getClusterLevel();
        }
        return -1;
    }
}
