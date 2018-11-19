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

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.result.ClusterAddResult;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.request.ClusterAddRequest;
import com.alibaba.nacossync.template.Processor;
import com.alibaba.nacossync.util.SkyWalkerUtil;

/**
 * @author NacosSync
 * @version $Id: ClusterAddProcessor.java, v 0.1 2018-09-30 下午12:22 NacosSync Exp $$
 */
@Slf4j
@Service
public class ClusterAddProcessor implements Processor<ClusterAddRequest, ClusterAddResult> {
    @Autowired
    private ClusterAccessService clusterAccessService;

    @Override
    public void process(ClusterAddRequest clusterAddRequest, ClusterAddResult clusterAddResult,
                        Object... others) throws Exception {
        ClusterDO clusterDO = new ClusterDO();

        if (!ClusterTypeEnum.contains(clusterAddRequest.getClusterType())) {

            throw new SkyWalkerException("集群类型不存在：" + clusterAddRequest.getClusterType());
        }

        String clusterId = SkyWalkerUtil.generateClusterId(clusterAddRequest);

        if (null != clusterAccessService.findByClusterId(clusterId)) {

            throw new SkyWalkerException("重复插入，clusterId已存在：" + clusterId);
        }

        clusterDO.setClusterId(clusterId);
        clusterDO.setClusterName(clusterAddRequest.getClusterName());
        clusterDO.setClusterType(clusterAddRequest.getClusterType());
        clusterDO.setConnectKeyList(JSONObject.toJSONString(clusterAddRequest.getConnectKeyList()));
        clusterAccessService.insert(clusterDO);
    }
}
