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

import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.request.ClusterAddRequest;
import com.alibaba.nacossync.pojo.result.ClusterAddResult;
import com.alibaba.nacossync.template.Processor;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

/**
 * @author NacosSync
 * @version $Id: ClusterAddProcessor.java, v 0.1 2018-09-30 PM12:22 NacosSync Exp $$
 */
@Slf4j
@Service
public class ClusterAddProcessor implements Processor<ClusterAddRequest, ClusterAddResult> {
    

    private final ClusterAccessService clusterAccessService;

    private final ObjectMapper objectMapper;
    
    public ClusterAddProcessor(ClusterAccessService clusterAccessService, ObjectMapper objectMapper) {
        this.clusterAccessService = clusterAccessService;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public void process(ClusterAddRequest clusterAddRequest, ClusterAddResult clusterAddResult,
        Object... others) throws Exception {
        ClusterDO clusterDO = new ClusterDO();

        if (null == clusterAddRequest.getConnectKeyList() || clusterAddRequest.getConnectKeyList().isEmpty()) {

            throw new SkyWalkerException("集群列表不能为空！");
        }

        if (StringUtils.isBlank(clusterAddRequest.getClusterName()) || StringUtils
            .isBlank(clusterAddRequest.getClusterType())) {

            throw new SkyWalkerException("集群名字或者类型不能为空！");
        }

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
        clusterDO.setConnectKeyList(objectMapper.writeValueAsString(clusterAddRequest.getConnectKeyList()));
        clusterDO.setUserName(clusterAddRequest.getUserName());
        clusterDO.setPassword(clusterAddRequest.getPassword());
        clusterDO.setNamespace(clusterAddRequest.getNamespace());
        clusterDO.setClusterLevel(0);
        clusterAccessService.insert(clusterDO);
    }
}
