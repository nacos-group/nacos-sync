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
package com.alibaba.nacossync.util;

import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author paderlol
 * @date: 2018-12-25 21:34
 */
public final class DubboConstants {
    public static final char ZOOKEEPER_SEPARATOR = '/';
    public static final String DUBBO_PATH_FORMAT =
        StringUtils.join(new String[] {"/dubbo", "%s", "providers"}, ZOOKEEPER_SEPARATOR);
    public static final String DUBBO_URL_FORMAT = "%s://%s:%s/%s?%s";
    public static final String VERSION_KEY = "version";
    public static final String GROUP_KEY = "group";
    public static final String INTERFACE_KEY = "interface";
    public static final String INSTANCE_IP_KEY = "ip";
    public static final String INSTANCE_PORT_KEY = "port";
    public static final String PROTOCOL_KEY = "protocol";
    public static final String WEIGHT_KEY = "weight";
    public static final String CATALOG_KEY = "providers";
    public static final String RELEASE_KEY = "release";
    public static final String SEPARATOR_KEY = ":";
    public static final int DUBBO_VERSION_INDEX = 2;
    public static final int MIN_DUBBO_VERSION = 2;
    public static final int MIDDLE_DUBBO_VERSION_INDEX = 3;
    public static final String RELEASE_SEPARATOR_KEY = ".";
    public static final BigDecimal COMPARE_NUMBER = new BigDecimal("7.2");

    public static final String DUBBO_ROOT_PATH = "/dubbo";
    public static final String ALL_SERVICE_NAME_PATTERN = "*";
    
    /**
     * Creates a service name based on Dubbo version compatibility.
     * if Dubbo version greater than 2.7.2, service name is providers:interface:version:
     * if Dubbo version less than 2.7.2, service name is providers:interface:version
     *
     * @param queryParam the query parameters that include keys such as interface, version, group, etc.
     * @return the constructed service name string
     */
    public static String createServiceName(Map<String, String> queryParam) {
        String group = queryParam.get(GROUP_KEY);
        String release = queryParam.get(RELEASE_KEY);
        
        String baseServiceName = Joiner.on(SEPARATOR_KEY).skipNulls().join(CATALOG_KEY, queryParam.get(INTERFACE_KEY),
                queryParam.get(VERSION_KEY), group);
        
        if (StringUtils.isBlank(group) && StringUtils.isNotBlank(release)) {
            List<String> versions = Splitter.on(RELEASE_SEPARATOR_KEY).splitToList(release);
            if (!CollectionUtils.isEmpty(versions) && versions.size() >= DUBBO_VERSION_INDEX) {
                String firstVersion = versions.get(0);
                String secondVersion = versions.get(1);
                BigDecimal bigDecimal = new BigDecimal(Joiner.on(RELEASE_SEPARATOR_KEY).join(secondVersion,
                        versions.size() > 2 ? versions.get(2) : "0"));
                if (isVersionRequiresSeparator(firstVersion, secondVersion, bigDecimal)) {
                    baseServiceName = baseServiceName.concat(SEPARATOR_KEY);
                }
            }
        }
        return baseServiceName;
    }
    
    /**
     * Checks if the version requires a separator to be appended to the service name.
     *
     * @param firstVersion  the major version
     * @param secondVersion the minor version
     * @param bigDecimal    the version number as BigDecimal
     * @return true if separator should be added, otherwise false
     */
    private static boolean isVersionRequiresSeparator(String firstVersion, String secondVersion, BigDecimal bigDecimal) {
        int majorVersion = Integer.parseInt(firstVersion);
        int minorVersion = Integer.parseInt(secondVersion);
        
        return (DUBBO_VERSION_INDEX == majorVersion && (MIDDLE_DUBBO_VERSION_INDEX <= minorVersion ||
                bigDecimal.compareTo(COMPARE_NUMBER) > 0)) || (MIN_DUBBO_VERSION < majorVersion);
    }

}
