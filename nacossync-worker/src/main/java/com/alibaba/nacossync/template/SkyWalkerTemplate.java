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
package com.alibaba.nacossync.template;

import lombok.extern.slf4j.Slf4j;

import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.pojo.request.BaseRequest;

/**
 * @author NacosSync
 * @version $Id: SkyWalkerTemplate.java, v 0.1 2018-05-12 PM1:13 NacosSync Exp $$
 */
@Slf4j
public class SkyWalkerTemplate {

    public static <T extends BaseResult> T run(Processor processor, BaseRequest request, T result,
                                               Object... others) {

        try {
            processor.process(request, result, others);

        } catch (Throwable e) {

            log.error("processor.process error", e);
            initExceptionResult(result, e);
        }

        return result;
    }

    private static <T extends BaseResult> void initExceptionResult(T result, Throwable e) {
        if (e instanceof SkyWalkerException) {
            SkyWalkerException skyWalkerException = (SkyWalkerException) e;
            if (null != skyWalkerException.getResultCode()) {
                result.setResultCode(skyWalkerException.getResultCode().getCode());
            }
        }
        result.setSuccess(Boolean.FALSE);
        result.setResultMessage(e.getMessage());
    }
}
