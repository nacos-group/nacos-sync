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
package com.alibaba.nacossync.cache;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.dao.ClusterAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.FinishedTask;
import com.alibaba.nacossync.pojo.model.ClusterDO;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import com.taobao.config.client.Subscriber;
import com.taobao.config.client.SubscriberRegistration;

/**
 * @author NacosSync
 * @version $Id: SkyWalkerCacheServices.java, v 0.1 2018-09-27 上午2:47 NacosSync Exp $$
 */
@Service
public class SkyWalkerCacheServices {

    @Autowired
    private ClusterAccessService clusterAccessService;

    private final ReentrantLock subCacheLock = new ReentrantLock();

    private static Map<String, FinishedTask> finishedTaskMap = new ConcurrentHashMap<>();

    private static Map<String, SubscriberRegistration> subRegistrationCache = new ConcurrentHashMap<>();

    private static Map<String, Subscriber> subscriberMapCache = new ConcurrentHashMap<>();

    public Subscriber getSubscriber(String key) {

        return subscriberMapCache.get(key);

    }

    public SubscriberRegistration putIfNotSubscriberRegistration(String key, String dataId) {

        subCacheLock.lock();
        SubscriberRegistration subscriberRegistration;
        try {
            subscriberRegistration = subRegistrationCache.get(key);

            if (null == subscriberRegistration) {

                subscriberRegistration = new SubscriberRegistration(key, dataId);
                subRegistrationCache.put(key, subscriberRegistration);
            }

        } finally {
            subCacheLock.unlock();
        }
        return subscriberRegistration;
    }

    public String getClusterConnectKey(String clusterId) {

        ClusterDO clusterDOS = clusterAccessService.findByClusterId(clusterId);

        List<String> connectKeyList = JSONObject.parseObject(clusterDOS.getConnectKeyList(),
                new TypeReference<List<String>>() {
                });

        if (CollectionUtils.isEmpty(connectKeyList)) {
            throw new SkyWalkerException("getClusterConnectKey empty, clusterId:" + clusterId);
        }

        Random random = new Random();
        return connectKeyList.get(random.nextInt(connectKeyList.size()));
    }
    
    public ClusterTypeEnum getClusterType(String clusterId) {

        ClusterDO clusterDOS = clusterAccessService.findByClusterId(clusterId);

        return ClusterTypeEnum.valueOf(clusterDOS.getClusterType());
    }

    public void addFinishedTask(TaskDO taskDO) {

        String operationId = SkyWalkerUtil.getOperationId(taskDO);

        FinishedTask finishedTask = new FinishedTask();
        finishedTask.setOperationId(operationId);

        finishedTaskMap.put(operationId, finishedTask);

    }

    public FinishedTask getFinishedTask(TaskDO taskDO) {

        String operationId = SkyWalkerUtil.getOperationId(taskDO);

        if (StringUtils.isEmpty(operationId)) {
            return null;
        }

        return finishedTaskMap.get(operationId);

    }

    public void addSubscriber(String taskId, Subscriber subscriber) {

        subscriberMapCache.put(taskId, subscriber);
    }

    public Map<String, FinishedTask> getFinishedTaskMap() {

        return finishedTaskMap;
    }

}
