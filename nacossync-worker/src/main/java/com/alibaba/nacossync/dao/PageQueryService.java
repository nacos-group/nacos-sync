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
import org.springframework.data.domain.Page;

/**
 * @author NacosSync
 * @version $Id: PageQueryService.java, v 0.1 2018年11月05日 PM5:51 NacosSync Exp $
 */
public interface PageQueryService<T> {

    Page<T> findPageNoCriteria(Integer pageNum, Integer size);

    Page<T> findPageCriteria(Integer pageNum, Integer size, QueryCondition queryCondition);

}