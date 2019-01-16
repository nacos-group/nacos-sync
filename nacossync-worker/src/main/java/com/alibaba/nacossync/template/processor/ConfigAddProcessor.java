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

import org.springframework.stereotype.Service;

import com.alibaba.nacossync.pojo.result.ConfigAddResult;
import com.alibaba.nacossync.pojo.request.ConfigAddRequest;
import com.alibaba.nacossync.template.Processor;

/**
 * @author NacosSync
 * @version $Id: ConfigAddProcessor.java, v 0.1 2018-09-30 PM5:02 NacosSync Exp $$
 */
@Service
public class ConfigAddProcessor implements Processor<ConfigAddRequest, ConfigAddResult> {
    @Override
    public void process(ConfigAddRequest configAddRequest, ConfigAddResult configAddResult,
                        Object... others) throws Exception {

    }
}
