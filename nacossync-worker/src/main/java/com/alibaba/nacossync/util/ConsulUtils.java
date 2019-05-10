package com.alibaba.nacossync.util;

import com.alibaba.nacos.client.naming.utils.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author paderlol
 * @date: 2019-04-25 00:01
 */
public class ConsulUtils {
    public static Map<String, String> transferMetadata(List<String> tags) {
        Map<String, String> metadata = new HashMap<>();
        if (CollectionUtils.isEmpty(tags)) {
            return tags.stream().filter(tag -> tag.split("=", -1).length == 2).map(tag -> tag.split("=", -1))
                .collect(Collectors.toMap(tagSplitArray -> tagSplitArray[0], tagSplitArray -> tagSplitArray[2]));
        }
        return metadata;
    }
}
