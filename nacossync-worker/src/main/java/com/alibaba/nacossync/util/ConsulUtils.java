package com.alibaba.nacossync.util;

import com.alibaba.nacos.client.naming.utils.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for handling Consul metadata.
 */
public class ConsulUtils {
    public static Map<String, String> transferMetadata(List<String> tags) {
        if (CollectionUtils.isEmpty(tags)) {
            return new HashMap<>();
        }
        
        return tags.stream()
                .map(tag -> tag.split("=", -1))
                .filter(tagArray -> tagArray.length == 2)
                .collect(Collectors.toMap(
                        tagArray -> tagArray[0],
                        tagArray -> tagArray[1],
                        (existing, replacement) -> existing // In case of duplicate keys, keep the existing value
                ));
    }
}
