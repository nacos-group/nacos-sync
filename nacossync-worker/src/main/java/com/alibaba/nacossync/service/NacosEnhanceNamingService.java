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
package com.alibaba.nacossync.service;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.client.naming.NacosNamingService;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.utils.HttpMethod;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacossync.pojo.request.CatalogServiceResult;
import org.springframework.util.ReflectionUtils;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.alibaba.nacossync.constant.SkyWalkerConstants.GROUP_NAME_PARAM;
import static com.alibaba.nacossync.constant.SkyWalkerConstants.PAGE_NO;
import static com.alibaba.nacossync.constant.SkyWalkerConstants.PAGE_SIZE;
import static com.alibaba.nacossync.constant.SkyWalkerConstants.SERVICE_NAME_PARAM;

/**
 * @author NacosSync
 * @since 2024-01-04 15:53:40
 */
public class NacosEnhanceNamingService {


    protected NamingService delegate;

    protected NamingProxy serverProxy;

    public NacosEnhanceNamingService(NamingService namingService) {
        if (!(namingService instanceof NacosNamingService)) {
            throw new IllegalArgumentException(
                    "namingService only support instance of com.alibaba.nacos.client.naming.NacosNamingService.");
        }
        this.delegate = namingService;

        // serverProxy
        final Field serverProxyField = ReflectionUtils.findField(NacosNamingService.class, "serverProxy");
        assert serverProxyField != null;
        ReflectionUtils.makeAccessible(serverProxyField);
        this.serverProxy = (NamingProxy) ReflectionUtils.getField(serverProxyField, delegate);
    }

    public CatalogServiceResult catalogServices(@Nullable String serviceName, @Nullable String group)
            throws NacosException {
        int pageNo = 1; // start with 1
        int pageSize = 100;

        final CatalogServiceResult result = catalogServices(serviceName, group, pageNo, pageSize);

        CatalogServiceResult tmpResult = result;

        while (Objects.nonNull(tmpResult) && tmpResult.getServiceList().size() >= pageSize) {
            pageNo++;
            tmpResult = catalogServices(serviceName, group, pageNo, pageSize);

            if (tmpResult != null) {
                result.getServiceList().addAll(tmpResult.getServiceList());
            }
        }

        return result;
    }

    /**
     * @see com.alibaba.nacos.client.naming.core.HostReactor#getServiceInfoDirectlyFromServer(String, String)
     */
    public CatalogServiceResult catalogServices(@Nullable String serviceName, @Nullable String group, int pageNo,
                                                                    int pageSize) throws NacosException {

        // pageNo
        // pageSize
        // serviceNameParam
        // groupNameParam
        final Map<String, String> params = new HashMap<>(8);
        params.put(CommonParams.NAMESPACE_ID, serverProxy.getNamespaceId());
        params.put(SERVICE_NAME_PARAM, serviceName);
        params.put(GROUP_NAME_PARAM, group);
        params.put(PAGE_NO, String.valueOf(pageNo));
        params.put(PAGE_SIZE, String.valueOf(pageSize));

        final String result = this.serverProxy.reqApi(UtilAndComs.nacosUrlBase + "/catalog/services", params,
                HttpMethod.GET);
        if (StringUtils.isNotEmpty(result)) {
            return JacksonUtils.toObj(result, CatalogServiceResult.class);
        }
        return null;
    }
}
