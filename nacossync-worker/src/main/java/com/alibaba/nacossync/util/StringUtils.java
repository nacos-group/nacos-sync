package com.alibaba.nacossync.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.nacossync.util.DubboConstants.INSTANCE_IP_KEY;
import static com.alibaba.nacossync.util.DubboConstants.INSTANCE_PORT_KEY;

/**
 * @author paderlol
 * @date: 2018-12-25 21:08
 */
public final class StringUtils {
    private static final Pattern KVP_PATTERN = Pattern.compile("([_.a-zA-Z0-9][-_.a-zA-Z0-9]*)[=](.*)");
    private static final Pattern IP_PORT_PATTERN = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+)\\:(\\d+)");

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
    public static Map<String, String> parseQueryString(String qs) throws UnsupportedEncodingException {
        String decodePath = URLDecoder.decode(qs, "UTF-8");
        if (isEmpty(qs)) {
            return new HashMap<String, String>();
        }
        return parseKeyValuePair(decodePath, "\\&");
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

    public static Map<String, String> parseIpAndPortString(String path) throws UnsupportedEncodingException {
        String decodePath = URLDecoder.decode(path, "UTF-8");
        Matcher matcher = IP_PORT_PATTERN.matcher(decodePath);
        // 将符合规则的提取出来
        Map<String, String> instanceMap = new HashMap<>();
        while (matcher.find()) {
            // ip地址
            instanceMap.put(INSTANCE_IP_KEY, matcher.group(1));
            // 端口
            instanceMap.put(INSTANCE_PORT_KEY, matcher.group(2));
            break;

        }
        return instanceMap;

    }
}
