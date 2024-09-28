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

package com.alibaba.nacossync.template.processor;

import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.request.ClusterDeleteRequest;
import com.alibaba.nacossync.pojo.result.ClusterDeleteResult;
import com.alibaba.nacossync.template.Processor;
import org.springframework.stereotype.Service;

/**
 * @author NacosSync
 * @version $Id: ClusterDeleteProcessor.java, v 0.1 2018-09-30 PM2:43 NacosSync Exp $$
 */
@Service
public class ClusterDeleteProcessor implements Processor<ClusterDeleteRequest, ClusterDeleteResult> {
    
    private final ClusterAccessService clusterAccessService;
    
    private final TaskAccessService taskAccessService;
    
    public ClusterDeleteProcessor(ClusterAccessService clusterAccessService, TaskAccessService taskAccessService) {
        this.clusterAccessService = clusterAccessService;
        this.taskAccessService = taskAccessService;
    }
    
    @Override
    public void process(ClusterDeleteRequest clusterDeleteRequest, ClusterDeleteResult clusterDeleteResult,
            Object... others) throws Exception {
        int count = taskAccessService.countByDestClusterIdOrSourceClusterId(clusterDeleteRequest.getClusterId(),
                clusterDeleteRequest.getClusterId());
        if (count > 0) {
            throw new SkyWalkerException(String.format("集群下有%d个任务，请先删除任务", count));
        }
        
        clusterAccessService.deleteByClusterId(clusterDeleteRequest.getClusterId());
        
    }
}
