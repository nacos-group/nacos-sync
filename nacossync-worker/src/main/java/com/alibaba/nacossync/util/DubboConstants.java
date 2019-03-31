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

import org.apache.commons.lang3.StringUtils;

import java.io.File;

/**
 * @author paderlol
 * @date: 2018-12-25 21:34
 */
public final class DubboConstants {
    public static final char ZOOKEEPER_SEPARATOR='/';
    public static final String DUBBO_PATH_FORMAT =
        StringUtils.join(new String[] {"/dubbo", "%s", "providers"}, ZOOKEEPER_SEPARATOR);
    public static final String DUBBO_URL_FORMAT ="%s://%s:%s/%s?%s";
    public static final String VERSION_KEY = "version";
    public static final String GROUP_KEY = "group";
    public static final String INTERFACE_KEY = "interface";
    public static final String INSTANCE_IP_KEY = "ip";
    public static final String INSTANCE_PORT_KEY = "port";
    public static final String PROTOCOL_KEY = "protocol";
    public static final String WEIGHT_KEY = "weight";
    public static final String CATALOG_KEY = "providers";
}
