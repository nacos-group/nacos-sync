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

/**
 * @author NacosSync
 * @version $Id: ResultCodeEnum.java, v 0.1 2018-09-25 PM4:38 NacosSync Exp $$
 */
public enum ResultCodeEnum {
    
    SUCCESS("SUCCESS", "request succeeded", "request succeeded"),
    SYSTEM_ERROR("SYSTEM_ERROR", "system exception", "system exception");

    private String code;
    private String errorMessage;
    private String detail;

    ResultCodeEnum(String code, String errorMessage, String detail) {
        this.code = code;
        this.errorMessage = errorMessage;
        this.detail = detail;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }
}
