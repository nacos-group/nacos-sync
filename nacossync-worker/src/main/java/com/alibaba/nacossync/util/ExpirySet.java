package com.alibaba.nacossync.util;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by maj on 2020/11/2.
 */
public class ExpirySet<T> {

    private Set<T> set = new CopyOnWriteArraySet();

    private Map<T, Long> expiryMap = new ConcurrentHashMap<T, Long>();

    private long timeOut = 10;//默认10s窗口期

    public ExpirySet() {

    }

    public ExpirySet(long timeout) {
        this.timeOut = timeout;
    }

    public boolean set(T key) {
        if (!set.add(key)) {
            if (getIntervalBySecond(expiryMap.get(key), new Date().getTime()) > timeOut) {
                expiryMap.putIfAbsent(key, new Date().getTime());
                return true;
            } else {
                return false;
            }
        } else {
            expiryMap.putIfAbsent(key, (new Date().getTime()));
            return true;
        }

    }

    public boolean remove(T key) {
        return set.remove(key);
    }

    public long getIntervalBySecond(long beforeTime, long currentTime) {
        return (currentTime - beforeTime) / 1000;
    }

}
