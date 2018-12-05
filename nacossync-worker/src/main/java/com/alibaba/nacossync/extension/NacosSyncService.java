package com.alibaba.nacossync.extension;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.Event;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.collect.ImmutableMap;
import io.swagger.models.auth.In;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yangyshdan
 * @version $Id: ConfigServerSyncManagerService.java, v 0.1 2018-11-12 下午5:17 NacosSync Exp $$
 */

@Slf4j
@Service
public class NacosSyncService implements SyncService, InitializingBean {
    private final ReentrantLock namingServiceMapLock = new ReentrantLock();
    private Map<String, EventListener> nacosListenerMap = new ConcurrentHashMap<>();
    private Map<String, NamingService> namingServiceMap = new ConcurrentHashMap<>();
    @Autowired
    private SyncManagerService syncManagerService;

    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            NamingService sourceNamingService = getFromCache(taskDO.getSourceClusterId(), taskDO.getGroupName());
            NamingService destNamingService = getFromCache(taskDO.getDestClusterId(), taskDO.getGroupName());

            sourceNamingService.unsubscribe(taskDO.getServiceName(), nacosListenerMap.get(taskDO.getTaskId()));

            // 删除目标集群中同步的实例列表
            List<Instance> instances = destNamingService.getAllInstances(taskDO.getServiceName());
            for (Instance instance : instances) {
                if (needDelete(instance.getMetadata(), taskDO)) {
                    destNamingService.deregisterInstance(taskDO.getServiceName(), instance.getIp(), instance.getPort());
                }
            }
        } catch (Exception e) {
            log.error("delete task fail, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            NamingService sourceNamingService = getFromCache(taskDO.getSourceClusterId(), taskDO.getGroupName());
            NamingService destNamingService = getFromCache(taskDO.getDestClusterId(), taskDO.getGroupName());

            nacosListenerMap.putIfAbsent(taskDO.getTaskId(), event -> {
                if (event instanceof NamingEvent) {
                    try {
                        Set instanceKeySet = new HashSet();
                        List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName());
                        // 先将新的注册一遍
                        for (Instance instance: sourceInstances) {
                            if (needSync(instance.getMetadata())) {
                                destNamingService.registerInstance(taskDO.getServiceName(), buildSyncInstance(instance, taskDO));
                                instanceKeySet.add(composeInstanceKey(instance));
                            }
                        }

                        // 再将不存在的删掉
                        List<Instance> destInstances = destNamingService.getAllInstances(taskDO.getServiceName());
                        for (Instance instance: destInstances) {
                            if (needDelete(instance.getMetadata(), taskDO) && !instanceKeySet.contains(composeInstanceKey(instance))) {
                                destNamingService.deregisterInstance(taskDO.getServiceName(), instance.getIp(), instance.getPort());
                            }
                        }
                    } catch (Exception e) {
                        log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                    }
                }
            });


            sourceNamingService.subscribe(taskDO.getServiceName(), nacosListenerMap.get(taskDO.getTaskId()));
        } catch (Exception e) {
            log.error("sync task fail, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    private String composeInstanceKey(Instance instance) {
        return instance.getIp() + ":" + instance.getPort();
    }


    private NamingService getFromCache(String clusterId, String namespace) throws NacosException {
        if (namespace == null) {
            namespace = "";
        }
        String key = clusterId + "_" + namespace;
        namingServiceMapLock.lock();
        try {
            if (namingServiceMap.get(key) == null) {
                Properties properties = new Properties();
                properties.setProperty(PropertyKeyConst.SERVER_ADDR, syncManagerService.skyWalkerCacheServices.getClusterConnectKey(clusterId));
                properties.setProperty(PropertyKeyConst.NAMESPACE, namespace);
                namingServiceMap.put(key, NamingFactory.createNamingService(properties));
            }
        } finally {
            namingServiceMapLock.unlock();
        }
        return namingServiceMap.get(key);
    }

    /**
     * 判断当前实例数据是否是其他地方同步过来的， 如果是则不进行同步操作
     * @param sourceMetaData
     * @return
     */
    private boolean needSync(Map<String, String> sourceMetaData) {
        if (StringUtils.isBlank(sourceMetaData.get(SkyWalkerConstants.SOURCE_CLUSTERID_KEY))) {
            return true;
        }
        return false;
    }

    /**
     * 判断当前实例数据是否源集群信息是一致的， 一致才会进行删除
     * @param destMetaData
     * @param taskDO
     * @return
     */
    private boolean needDelete(Map<String, String> destMetaData, TaskDO taskDO) {
        if (StringUtils.equals(destMetaData.get(SkyWalkerConstants.SOURCE_CLUSTERID_KEY), taskDO.getSourceClusterId())) {
            return true;
        }
        return false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        syncManagerService.register(ClusterTypeEnum.NACOS, this);
    }

    public Instance buildSyncInstance(Instance instance, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(instance.getIp());
        temp.setPort(instance.getPort());
        temp.setClusterName(instance.getClusterName());
        temp.setServiceName(instance.getServiceName());
        temp.setEnabled(instance.isEnabled());
        temp.setHealthy(instance.isHealthy());
        temp.setWeight(instance.getWeight());

        Map<String, String> metaData = new HashMap<>();
        metaData.putAll(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY, syncManagerService.skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }
}
