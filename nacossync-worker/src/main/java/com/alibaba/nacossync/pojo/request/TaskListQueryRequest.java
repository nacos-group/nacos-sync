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
package com.alibaba.nacossync.pojo.request;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author NacosSync
 * @version $Id: TaskListRequest.java, v 0.1 2018-09-30 PM12:58 NacosSync Exp $$
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TaskListQueryRequest extends BaseRequest {

    private Integer pageNum;
    private Integer pageSize;
    /**
     * When querying the service list, if this value is not empty then the fuzzy match query field
     * of service name
     */
    private String serviceName;
}
