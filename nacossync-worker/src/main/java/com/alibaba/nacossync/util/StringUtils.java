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

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.nacossync.util.DubboConstants.*;

/**
 * @author paderlol
 * @date: 2018-12-25 21:08
 */
@Slf4j
public final class StringUtils {

    private static final Pattern KVP_PATTERN = Pattern
            .compile("([_.a-zA-Z0-9][-_.a-zA-Z0-9]*)[=](.*)");
    private static final Pattern IP_PORT_PATTERN = Pattern
            .compile(".*/(.*)://(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)");
	private static final Pattern DUBBO_PROVIDER_PATTERN = Pattern
            .compile("/dubbo/(.*)/providers/(.*)");

    /**
     * parse key-value pair.
     *
     * @param str string.
     * @param itemSeparator item separator.
     * @return key-value map;
     */
    private static Map<String, String> parseKeyValuePair(String str, String itemSeparator) {
        String[] tmp = str.split(itemSeparator);
        Map<String, String> map = new HashMap<String, String>(tmp.length);
        for (int i = 0; i < tmp.length; i++) {
            Matcher matcher = KVP_PATTERN.matcher(tmp[i]);
            if (!matcher.matches()) {
                continue;
            }
            map.put(matcher.group(1), matcher.group(2));
        }
        return map;
    }

    /**
     * parse query string to Parameters.
     *
     * @param qs query string.
     * @return Parameters instance.
     */
    public static Map<String, String> parseQueryString(String qs) {
        try {
            String decodePath = URLDecoder.decode(qs, "UTF-8");
            if (isEmpty(qs)) {
                return new HashMap<>();
            }
            return parseKeyValuePair(decodePath, "\\&");

        } catch (UnsupportedEncodingException e) {
            log.warn("parse query string failed", e);
            return Maps.newHashMap();
        }
    }

    /**
     * is empty string.
     *
     * @param str source string.
     * @return is empty.
     */
    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public static Map<String, String> parseIpAndPortString(String path) {

        try {
            String decodePath = URLDecoder.decode(path, "UTF-8");
            Matcher matcher = IP_PORT_PATTERN.matcher(decodePath);
            // extract the ones that match the rules
            Map<String, String> instanceMap = new HashMap<>(3);
            while (matcher.find()) {
                // protocol
                instanceMap.put(PROTOCOL_KEY, matcher.group(1));
                // ip address
                instanceMap.put(INSTANCE_IP_KEY, matcher.group(2));
                // port
                instanceMap.put(INSTANCE_PORT_KEY, matcher.group(3));
                break;
            }
            return instanceMap;
        } catch (UnsupportedEncodingException e) {
            log.warn("parse query string failed", e);
            return Maps.newHashMap();
        }

    }

    public static String convertDubboProvidersPath(String interfaceName) {
        return String.format(DUBBO_PATH_FORMAT, interfaceName);
    }

	public static String convertDubboFullPathForZk(Map<String, String> metaData, String providersPath, String ip,
			int port) {
		try {
			String urlParam = Joiner.on("&").withKeyValueSeparator("=").join(metaData);
			String instanceUrl = String.format(DUBBO_URL_FORMAT, metaData.get(PROTOCOL_KEY), ip, port,
					metaData.get(INTERFACE_KEY), urlParam);

			return Joiner.on(ZOOKEEPER_SEPARATOR).join(providersPath, URLEncoder.encode(instanceUrl, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			log.warn("convert Dubbo full path", e);
			return "";
		}

	}

	public static boolean isDubboProviderPath(String path) {
		return DUBBO_PROVIDER_PATTERN.matcher(path).matches();
	}
}
