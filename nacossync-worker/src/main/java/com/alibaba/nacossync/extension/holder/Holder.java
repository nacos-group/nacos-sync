package com.alibaba.nacossync.extension.holder;

/**
 * @author paderlol
 * @date: 2018-12-24 21:59
 */
public interface Holder<T> {

    public T get(String clusterId, String namespace) throws Exception;
}
