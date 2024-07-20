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

package com.alibaba.nacossync.api;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.nacossync.pojo.result.ConfigAddResult;
import com.alibaba.nacossync.pojo.result.ConfigDeleteResult;
import com.alibaba.nacossync.pojo.result.ConfigQueryResult;
import com.alibaba.nacossync.pojo.request.ConfigAddRequest;
import com.alibaba.nacossync.pojo.request.ConfigDeleteRequest;
import com.alibaba.nacossync.pojo.request.ConfigQueryRequest;
import com.alibaba.nacossync.template.SkyWalkerTemplate;
import com.alibaba.nacossync.template.processor.ConfigAddProcessor;
import com.alibaba.nacossync.template.processor.ConfigDeleteProcessor;
import com.alibaba.nacossync.template.processor.ConfigQueryProcessor;

/**
 * @author NacosSync
 * @version $Id: SystemConfigApi.java, v 0.1 2018-09-26 AM2:06 NacosSync Exp $$
 */
@Slf4j
@RestController
public class SystemConfigApi {
    
    private final ConfigQueryProcessor configQueryProcessor;
    
    private final ConfigDeleteProcessor configDeleteProcessor;
    
    private final ConfigAddProcessor configAddProcessor;
    
    public SystemConfigApi(ConfigQueryProcessor configQueryProcessor, ConfigDeleteProcessor configDeleteProcessor,
            ConfigAddProcessor configAddProcessor) {
        this.configQueryProcessor = configQueryProcessor;
        this.configDeleteProcessor = configDeleteProcessor;
        this.configAddProcessor = configAddProcessor;
    }
    
    @RequestMapping(path = "/v1/systemconfig/list", method = RequestMethod.GET)
    public ConfigQueryResult tasks(ConfigQueryRequest configQueryRequest) {
        
        return SkyWalkerTemplate.run(configQueryProcessor, configQueryRequest, new ConfigQueryResult());
    }
    
    @RequestMapping(path = "/v1/systemconfig/delete", method = RequestMethod.DELETE)
    public ConfigDeleteResult deleteTask(@RequestBody ConfigDeleteRequest configDeleteRequest) {
        
        return SkyWalkerTemplate.run(configDeleteProcessor, configDeleteRequest, new ConfigDeleteResult());
    }
    
    @RequestMapping(path = "/v1/systemconfig/add", method = RequestMethod.POST)
    public ConfigAddResult taskAdd(@RequestBody ConfigAddRequest configAddRequest) {
        
        return SkyWalkerTemplate.run(configAddProcessor, configAddRequest, new ConfigAddResult());
    }
    
}
