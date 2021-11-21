package com.alibaba.nacossync.util;

import com.alibaba.nacos.api.common.Constants;
import org.apache.commons.lang3.StringUtils;

public class NacosUtils {

    public static String getGroupNameOrDefault(String groupName) {
        return StringUtils.defaultIfBlank(groupName, Constants.DEFAULT_GROUP);
    }
}
