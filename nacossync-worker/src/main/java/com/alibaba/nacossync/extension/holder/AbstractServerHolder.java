package com.alibaba.nacossync.extension.holder;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author paderlol
 * @date: 2018-12-24 22:08
 */
public abstract class AbstractServerHolder<T> implements Holder {
    private final ReentrantLock serviceMapLock = new ReentrantLock();
    private Map<String, T> serviceMap = new ConcurrentHashMap<>();
    @Autowired
    protected SkyWalkerCacheServices skyWalkerCacheServices;

    @Override
    public T get(String clusterId, String namespace) throws Exception {
        if (namespace == null) {
            namespace = "";
        }
        String key = clusterId + "_" + namespace;
        serviceMapLock.lock();
        try {
            if (serviceMap.get(key) == null) {
                serviceMap.put(key, createServer(skyWalkerCacheServices.getClusterConnectKey(clusterId), namespace));
            }
        } finally {
            serviceMapLock.unlock();
        }
        return serviceMap.get(key);
    }

     abstract T createServer(String serverAddress, String namespace) throws Exception;
}
