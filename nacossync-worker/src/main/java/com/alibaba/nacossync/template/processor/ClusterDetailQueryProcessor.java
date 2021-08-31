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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.pojo.result.ClusterDetailQueryResult;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.request.ClusterDetailQueryRequest;
import com.alibaba.nacossync.pojo.view.ClusterModel;
import com.alibaba.nacossync.template.Processor;

/**
 * @author NacosSync
 * @version $Id: ClusterDetailQueryProcessor.java, v 0.1 2018-09-30 PM2:39 NacosSync Exp $$
 */
@Service
public class ClusterDetailQueryProcessor
                                        implements
                                        Processor<ClusterDetailQueryRequest, ClusterDetailQueryResult> {
    @Autowired
    private ClusterAccessService clusterAccessService;

    @Override
    public void process(ClusterDetailQueryRequest clusterDetailQueryRequest,
                        ClusterDetailQueryResult clusterDetailQueryResult, Object... others)
                                                                                            throws Exception {

        ClusterDO clusterDO = clusterAccessService.findByClusterId(clusterDetailQueryRequest
            .getClusterId());

        ClusterModel clusterModel = new ClusterModel();
        clusterModel.setClusterId(clusterDO.getClusterId());
        clusterModel.setConnectKeyList(clusterDO.getConnectKeyList());
        clusterModel.setClusterType(clusterDO.getClusterType());
        clusterModel.setClusterName(clusterDO.getClusterName());
        clusterModel.setNamespace(clusterDO.getNamespace());

        clusterDetailQueryResult.setClusterModel(clusterModel);

    }
}
