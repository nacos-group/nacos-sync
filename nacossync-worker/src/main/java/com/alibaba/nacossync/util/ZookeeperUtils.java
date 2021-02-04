package com.alibaba.nacossync.util;

import java.util.Iterator;
import java.util.List;

import static com.alibaba.nacossync.util.DubboConstants.IGNORED_DUBBO_PATH;

/**
 * Created by maj on 2021/2/4.
 */
public class ZookeeperUtils {

    public static List<String> filterNoProviderPath(List<String> sourceInstances) {
        Iterator<String> iterator = sourceInstances.iterator();
        while (iterator.hasNext()) {
            if (IGNORED_DUBBO_PATH.contains(iterator.next())) {
                iterator.remove();
            }
        }
        return sourceInstances;
    }
}
