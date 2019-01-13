/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.alibaba.nacossync.extension;

import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.pojo.model.TaskDO;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public interface SyncService {

    public boolean delete(TaskDO taskDO);

    public boolean sync(TaskDO taskDO);

    /**
     * 判断当前实例数据是否是其他地方同步过来的， 如果是则不进行同步操作
     *
     * @param sourceMetaData
     * @return
     */
    default boolean needSync(Map<String, String> sourceMetaData) {
        return StringUtils.isBlank(sourceMetaData.get(SkyWalkerConstants.SOURCE_CLUSTERID_KEY));
    }

    /**
     * 判断当前实例数据是否源集群信息是一致的， 一致才会进行删除
     *
     * @param destMetaData
     * @param taskDO
     * @return
     */
    default boolean needDelete(Map<String, String> destMetaData, TaskDO taskDO) {
        return StringUtils.equals(destMetaData.get(SkyWalkerConstants.SOURCE_CLUSTERID_KEY),
            taskDO.getSourceClusterId());
    }



}
