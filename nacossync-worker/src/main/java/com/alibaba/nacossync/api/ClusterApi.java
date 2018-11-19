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
package com.alibaba.nacossync.api;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.pojo.result.*;
import com.alibaba.nacossync.pojo.request.ClusterAddRequest;
import com.alibaba.nacossync.pojo.request.ClusterDeleteRequest;
import com.alibaba.nacossync.pojo.request.ClusterDetailQueryRequest;
import com.alibaba.nacossync.pojo.request.ClusterListQueryRequest;
import com.alibaba.nacossync.template.SkyWalkerTemplate;
import com.alibaba.nacossync.template.processor.ClusterAddProcessor;
import com.alibaba.nacossync.template.processor.ClusterDeleteProcessor;
import com.alibaba.nacossync.template.processor.ClusterDetailQueryProcessor;
import com.alibaba.nacossync.template.processor.ClusterListQueryProcessor;

/**
 * @author NacosSync
 * @version $Id: ClusterApi.java, v 0.1 2018-09-25 下午9:30 NacosSync Exp $$
 */
@Slf4j
@RestController
public class ClusterApi {

    @Autowired
    private ClusterAddProcessor clusterAddProcessor;

    @Autowired
    private ClusterDeleteProcessor clusterDeleteProcessor;

    @Autowired
    private ClusterDetailQueryProcessor clusterDetailQueryProcessor;

    @Autowired
    private ClusterListQueryProcessor clusterListQueryProcessor;

    @RequestMapping(path = "/v1/cluster/list", method = RequestMethod.GET)
    public ClusterListQueryResult clusters(ClusterListQueryRequest clusterListQueryRequest) {

        return SkyWalkerTemplate.run(clusterListQueryProcessor, clusterListQueryRequest,
                new ClusterListQueryResult());
    }

    @RequestMapping(path = "/v1/cluster/detail", method = RequestMethod.GET)
    public ClusterDetailQueryResult getByTaskId(ClusterDetailQueryRequest clusterDetailQueryRequest) {

        return SkyWalkerTemplate.run(clusterDetailQueryProcessor, clusterDetailQueryRequest,
                new ClusterDetailQueryResult());
    }

    @RequestMapping(path = "/v1/cluster/delete", method = RequestMethod.DELETE)
    public ClusterDeleteResult deleteTask(@RequestBody ClusterDeleteRequest clusterDeleteRequest) {

        return SkyWalkerTemplate.run(clusterDeleteProcessor, clusterDeleteRequest,
                new ClusterDeleteResult());

    }

    @RequestMapping(path = "/v1/cluster/add", method = RequestMethod.POST)
    public ClusterAddResult clusterAdd(@RequestBody ClusterAddRequest clusterAddRequest) {

        return SkyWalkerTemplate
                .run(clusterAddProcessor, clusterAddRequest, new ClusterAddResult());
    }

    @RequestMapping(path = "/v1/cluster/types", method = RequestMethod.GET)
    public ClusterTypeResult getClusterType() {

        return new ClusterTypeResult(ClusterTypeEnum.getClusterTypeCodes());
    }

}
