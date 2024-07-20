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
package com.alibaba.nacossync.constant;

import lombok.Getter;

/**
 * @author NacosSync
 * @version $Id: TaskStatusEnum.java, v 0.1 2018-09-26 上午2:38 NacosSync Exp $$
 */
public enum TaskStatusEnum {

    /**
     * synchronization of task
     */
    SYNC("SYNC", "任务同步"),
    /**
     * delete the task
     */
    DELETE("DELETE", "任务需要被删除");
    

    @Getter
    private final String code;
    private final String desc;

    TaskStatusEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }
    
    
    public static boolean contains(String code) {

        for (TaskStatusEnum taskStatusEnum : TaskStatusEnum.values()) {

            if (taskStatusEnum.getCode().equals(code)) {
                return true;
            }
        }
        return false;
    }

}
