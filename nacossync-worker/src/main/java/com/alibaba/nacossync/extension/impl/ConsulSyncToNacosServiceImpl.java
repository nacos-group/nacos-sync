package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.event.SpecialSyncEventBus;
import com.alibaba.nacossync.extension.holder.ConsulServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.model.HealthService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

/**
 * Consul 同步 Nacos
 * 
 * @author paderlol
 * @date: 2018-12-31 16:25
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.CONSUL, destinationCluster = ClusterTypeEnum.NACOS)
public class ConsulSyncToNacosServiceImpl implements SyncService {

    private final ConsulServerHolder consulServerHolder;
    private final SkyWalkerCacheServices skyWalkerCacheServices;

    private final NacosServerHolder nacosServerHolder;

    private final SpecialSyncEventBus specialSyncEventBus;

    @Autowired
    public ConsulSyncToNacosServiceImpl(ConsulServerHolder consulServerHolder,
        SkyWalkerCacheServices skyWalkerCacheServices, NacosServerHolder nacosServerHolder,
        SpecialSyncEventBus specialSyncEventBus) {
        this.consulServerHolder = consulServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.specialSyncEventBus = specialSyncEventBus;
    }

    @Override
    public boolean delete(TaskDO taskDO) {

        try {
            specialSyncEventBus.unsubscribe(taskDO);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName());
            for (Instance instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO)) {
                    destNamingService.deregisterInstance(taskDO.getServiceName(), instance.getIp(), instance.getPort());
                }
            }

        } catch (Exception e) {
            log.error("delete task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            ConsulClient consulClient = consulServerHolder.get(taskDO.getSourceClusterId(), null);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            Response<List<HealthService>> response =
                consulClient.getHealthServices(taskDO.getServiceName(), true, QueryParams.DEFAULT);
            List<HealthService> healthServiceList = response.getValue();
            Set<String> instanceKeySet = new HashSet<>();
            for (HealthService healthService : healthServiceList) {
                if (needSync(healthService.getNode().getMeta())) {

                    destNamingService.registerInstance(taskDO.getServiceName(),
                        buildSyncInstance(healthService, taskDO));
                    instanceKeySet.add(composeInstanceKey(healthService.getService().getAddress(),
                        healthService.getService().getPort()));
                }
            }
            List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName());
            for (Instance instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO)
                    && !instanceKeySet.contains(composeInstanceKey(instance.getIp(), instance.getPort()))) {
                    destNamingService.deregisterInstance(taskDO.getServiceName(), instance.getIp(), instance.getPort());
                }
            }
            specialSyncEventBus.subscribe(taskDO, this::sync);
        } catch (Exception e) {
            log.error("sync task from consul to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    private Instance buildSyncInstance(HealthService instance, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(instance.getService().getAddress());
        temp.setPort(instance.getService().getPort());

        Map<String, String> metaData = new HashMap<>(instance.getNode().getMeta());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }

    private String composeInstanceKey(String ip, int port) {
        return ip + ":" + port;
    }

}
