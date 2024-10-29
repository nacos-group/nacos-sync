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
import lombok.extern.slf4j.Slf4j;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.nacossync.util.DubboConstants.DUBBO_PATH_FORMAT;
import static com.alibaba.nacossync.util.DubboConstants.DUBBO_URL_FORMAT;
import static com.alibaba.nacossync.util.DubboConstants.INSTANCE_IP_KEY;
import static com.alibaba.nacossync.util.DubboConstants.INSTANCE_PORT_KEY;
import static com.alibaba.nacossync.util.DubboConstants.INTERFACE_KEY;
import static com.alibaba.nacossync.util.DubboConstants.PROTOCOL_KEY;
import static com.alibaba.nacossync.util.DubboConstants.ZOOKEEPER_SEPARATOR;

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
    public static final int INDEX_NOT_FOUND = -1;
    public static final String EMPTY = "";

    /**
     * parse key-value pair.
     *
     * @param str           string.
     * @param itemSeparator item separator.
     * @return key-value map;
     */
    private static Map<String, String> parseKeyValuePair(String str, String itemSeparator) {
        String[] tmp = str.split(itemSeparator);
        Map<String, String> map = new HashMap<>(tmp.length);
        for (String s : tmp) {
            Matcher matcher = KVP_PATTERN.matcher(s);
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
        
        String decodePath = URLDecoder.decode(qs, StandardCharsets.UTF_8);
        if (isEmpty(decodePath)) {
            return new HashMap<>();
        }
        decodePath = substringAfter(decodePath, "?");
        return parseKeyValuePair(decodePath, "&");
        
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
        
        String decodePath = URLDecoder.decode(path, StandardCharsets.UTF_8);
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
        
    }

    /**
     * <p>Gets the substring after the first occurrence of a separator.
     * The separator is not returned.</p>
     *
     * <p>A {@code null} string input will return {@code null}.
     * An empty ("") string input will return the empty string. A {@code null} separator will return the empty string if
     * the input string is not {@code null}.</p>
     *
     * <p>If nothing is found, the empty string is returned.</p>
     *
     * <pre>
     * StringUtils.substringAfter(null, *)      = null
     * StringUtils.substringAfter("", *)        = ""
     * StringUtils.substringAfter(*, null)      = ""
     * StringUtils.substringAfter("abc", "a")   = "bc"
     * StringUtils.substringAfter("abcba", "b") = "cba"
     * StringUtils.substringAfter("abc", "c")   = ""
     * StringUtils.substringAfter("abc", "d")   = ""
     * StringUtils.substringAfter("abc", "")    = "abc"
     * </pre>
     *
     * @param str       the String to get a substring from, may be null
     * @param separator the String to search for, may be null
     * @return the substring after the first occurrence of the separator, {@code null} if null String input
     * @since 2.0
     */
    public static String substringAfter(final String str, final String separator) {
        if (isEmpty(str)) {
            return str;
        }
        if (separator == null) {
            return EMPTY;
        }
        final int pos = str.indexOf(separator);
        if (pos == INDEX_NOT_FOUND) {
            return EMPTY;
        }
        return str.substring(pos + 1);
    }

    public static String convertDubboProvidersPath(String interfaceName) {
        return String.format(DUBBO_PATH_FORMAT, interfaceName);
    }

    public static String convertDubboFullPathForZk(Map<String, String> metaData, String providersPath, String ip,
        int port) {
        String urlParam = Joiner.on("&").withKeyValueSeparator("=").join(metaData);
        String instanceUrl = String.format(DUBBO_URL_FORMAT, metaData.get(PROTOCOL_KEY), ip, port,
            metaData.get(INTERFACE_KEY), urlParam);
        
        return Joiner.on(ZOOKEEPER_SEPARATOR).join(providersPath, URLEncoder.encode(instanceUrl, StandardCharsets.UTF_8));
        
    }

    public static boolean isDubboProviderPath(String path) {
        return DUBBO_PROVIDER_PATTERN.matcher(path).matches();
    }
}
