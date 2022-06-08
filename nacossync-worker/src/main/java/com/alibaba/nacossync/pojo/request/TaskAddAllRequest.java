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

/**
 * @author NacosSync
 * @version $Id: TaskAddAllRequest.java, v 0.1 2022-03-23 AM12:13 NacosSync Exp $$
 */
@Data
public class TaskAddAllRequest extends BaseRequest {
    
    /**
     * eg: b7bacb110199d5bb83b9757038fadeb0 .
     */
    private String sourceClusterId;
    
    /**
     * eg: bbdad57833a0e4f0981f6f3349005617 .
     */
    private String destClusterId;
    
    /**
     * whether to exclude subscriber.
     */
    private boolean excludeConsumer = true;
    
}
