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
package com.alibaba.nacossync.extension;

import java.util.Hashtable;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.pojo.model.TaskDO;

/**
 * @author NacosSync
 * @version $Id: SyncManagerService.java, v 0.1 2018-09-25 下午5:17 NacosSync Exp $$
 */
@Slf4j
@Service
public class SyncManagerService {

    @Autowired
    protected SkyWalkerCacheServices skyWalkerCacheServices;

    private Hashtable<ClusterTypeEnum, SyncService> syncServiceMap = new Hashtable<ClusterTypeEnum, SyncService>();

    public void register(ClusterTypeEnum type, SyncService service) {
        syncServiceMap.put(type, service);
    }

    public boolean delete(TaskDO taskDO) throws NacosException {

        ClusterTypeEnum type = this.skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId());
        return syncServiceMap.get(type).delete(taskDO);

    }

    public boolean sync(TaskDO taskDO) {

        ClusterTypeEnum type = this.skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId());

        return syncServiceMap.get(type).sync(taskDO);

    }

}
