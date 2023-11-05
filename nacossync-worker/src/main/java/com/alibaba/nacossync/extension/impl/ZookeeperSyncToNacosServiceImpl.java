/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.MetricsStatisticsType;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static com.alibaba.nacossync.util.DubboConstants.ALL_SERVICE_NAME_PATTERN;
import static com.alibaba.nacossync.util.DubboConstants.DUBBO_PATH_FORMAT;
import static com.alibaba.nacossync.util.DubboConstants.DUBBO_ROOT_PATH;
import static com.alibaba.nacossync.util.DubboConstants.GROUP_KEY;
import static com.alibaba.nacossync.util.DubboConstants.INSTANCE_IP_KEY;
import static com.alibaba.nacossync.util.DubboConstants.INSTANCE_PORT_KEY;
import static com.alibaba.nacossync.util.DubboConstants.INTERFACE_KEY;
import static com.alibaba.nacossync.util.DubboConstants.PROTOCOL_KEY;
import static com.alibaba.nacossync.util.DubboConstants.VERSION_KEY;
import static com.alibaba.nacossync.util.DubboConstants.WEIGHT_KEY;
import static com.alibaba.nacossync.util.DubboConstants.ZOOKEEPER_SEPARATOR;
import static com.alibaba.nacossync.util.DubboConstants.createServiceName;
import static com.alibaba.nacossync.util.NacosUtils.getGroupNameOrDefault;
import static com.alibaba.nacossync.util.StringUtils.parseIpAndPortString;
import static com.alibaba.nacossync.util.StringUtils.parseQueryString;

/**
 * @author paderlol
 * @version 1.0
 * @date: 2018-12-24 21:33
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.ZK, destinationCluster = ClusterTypeEnum.NACOS)
public class ZookeeperSyncToNacosServiceImpl implements SyncService {
    
    @Autowired
    private MetricsManager metricsManager;
    
    /**
     * Listener cache of Zookeeper format taskId -> PathChildrenCache instance
     */
    private Map<String, TreeCache> treeCacheMap = new ConcurrentHashMap<>();
    
    /**
     * service name cache
     */
    private Map<String, String> nacosServiceNameMap = new ConcurrentHashMap<>();
    
    private final ZookeeperServerHolder zookeeperServerHolder;
    
    private final NacosServerHolder nacosServerHolder;
    
    private final SkyWalkerCacheServices skyWalkerCacheServices;
    
    @Autowired
    public ZookeeperSyncToNacosServiceImpl(ZookeeperServerHolder zookeeperServerHolder,
            NacosServerHolder nacosServerHolder, SkyWalkerCacheServices skyWalkerCacheServices) {
        this.zookeeperServerHolder = zookeeperServerHolder;
        this.nacosServerHolder = nacosServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
    }
    
    @Override
    public boolean sync(TaskDO taskDO, Integer index) {
        try {
            if (treeCacheMap.containsKey(taskDO.getTaskId())) {
                return true;
            }
            
            TreeCache treeCache = getTreeCache(taskDO);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());
            // 初次执行任务统一注册所有实例
            registerAllInstances(taskDO, destNamingService);
            //注册ZK监听
            Objects.requireNonNull(treeCache).getListenable().addListener((client, event) -> {
                try {
                    
                    String path = event.getData().getPath();
                    Map<String, String> queryParam = parseQueryString(path);
                    if (isMatch(taskDO, queryParam) && needSync(queryParam)) {
                        processEvent(taskDO, destNamingService, event, path, queryParam);
                    }
                } catch (Exception e) {
                    log.error("event process from Zookeeper to Nacos was failed, taskId:{}", taskDO.getTaskId(), e);
                    metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
                }
                
            });
        } catch (Exception e) {
            log.error("sync task from Zookeeper to Nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.SYNC_ERROR);
            return false;
        }
        return true;
    }
    
    private void processEvent(TaskDO taskDO, NamingService destNamingService, TreeCacheEvent event, String path,
            Map<String, String> queryParam) throws NacosException {
        if (!com.alibaba.nacossync.util.StringUtils.isDubboProviderPath(path)) {
            return;
        }
        
        Map<String, String> ipAndPortParam = parseIpAndPortString(path);
        Instance instance = buildSyncInstance(queryParam, ipAndPortParam, taskDO);
        String serviceName = queryParam.get(INTERFACE_KEY);
        String groupName = getGroupNameOrDefault(taskDO.getGroupName());
        switch (event.getType()) {
            case NODE_ADDED:
            case NODE_UPDATED:
                String registerNacosServiceName = getServiceNameFromCache(serviceName, queryParam);
                if (instance.isEphemeral()) {
                    List<Instance> destAllInstances = destNamingService.getAllInstances(registerNacosServiceName,
                            groupName, new ArrayList<>(), true);
                    List<Instance> needRegisterInstances = new ArrayList<>();
                    for (Instance destInstance : destAllInstances) {
                        if (needDelete(destInstance.getMetadata(), taskDO)) {
                            // 从当前源集群同步到目标集群的实例
                            if (!instanceEquals(instance, destInstance) ) {
                                log.debug("需要从源集群同步到目标集群的临时实例：{}", destInstance);
                                needRegisterInstances.add(instance);
                            }
                        }
                    }
                    log.debug("需要从源集群更新到目标集群的临时实例：{}", instance);
                    needRegisterInstances.add(instance);
                    log.debug("将源集群指定service的临时实例全量同步到目标集群的{}: {}", registerNacosServiceName, taskDO);
                    destNamingService.batchRegisterInstance(registerNacosServiceName, groupName, needRegisterInstances);
                } else {
                    log.debug("将源集群指定service的持久实例{}同步到目标集群的{}: {}", instance, registerNacosServiceName, taskDO);
                    destNamingService.registerInstance(registerNacosServiceName, groupName, instance);
                }
                break;
            case NODE_REMOVED:
                String deregisterNacosServiceName = getServiceNameFromCache(serviceName, queryParam);
                log.debug("反注册目标集群的{}: {}", deregisterNacosServiceName, taskDO);
                destNamingService.deregisterInstance(deregisterNacosServiceName,
                        groupName, ipAndPortParam.get(INSTANCE_IP_KEY),
                        Integer.parseInt(ipAndPortParam.get(INSTANCE_PORT_KEY)));
                nacosServiceNameMap.remove(serviceName);
                break;
            default:
                break;
        }
    }
    
    private void registerAllInstances(TaskDO taskDO, NamingService destNamingService) throws Exception {
        CuratorFramework zk = zookeeperServerHolder.get(taskDO.getSourceClusterId());
        if (!ALL_SERVICE_NAME_PATTERN.equals(taskDO.getServiceName())) {
            registerALLInstances0(taskDO, destNamingService, zk, taskDO.getServiceName());
        } else {
            // 同步全部
            List<String> serviceList = zk.getChildren().forPath(DUBBO_ROOT_PATH);
            for (String serviceName : serviceList) {
                registerALLInstances0(taskDO, destNamingService, zk, serviceName);
            }
        }
    }
    
    private void registerALLInstances0(TaskDO taskDO, NamingService destNamingService, CuratorFramework zk,
            String serviceName) throws Exception {
        String path = String.format(DUBBO_PATH_FORMAT, serviceName);
        if (zk.getChildren() == null) {
            return;
        }
        Map<String, List<Instance>> needRegisterInstanceMap = new HashMap<>();
        List<String> providers = zk.getChildren().forPath(path);
        for (String provider : providers) {
            Map<String, String> queryParam = parseQueryString(provider);
            if (isMatch(taskDO, queryParam) && needSync(queryParam)) {
                Map<String, String> ipAndPortParam = parseIpAndPortString(path + ZOOKEEPER_SEPARATOR + provider);
                Instance instance = buildSyncInstance(queryParam, ipAndPortParam, taskDO);
                String nacosServiceName = getServiceNameFromCache(serviceName, queryParam);
                List<Instance> needRegisterInstances = needRegisterInstanceMap.get(nacosServiceName);
                if (needRegisterInstances == null) {
                    needRegisterInstances = new ArrayList<>();
                    needRegisterInstances.add(instance);
                    needRegisterInstanceMap.put(nacosServiceName, needRegisterInstances);
                } else {
                    needRegisterInstances.add(instance);
                }
            }
        }
        String groupName = taskDO.getGroupName();
        for (Map.Entry<String, List<Instance>> entry : needRegisterInstanceMap.entrySet()) {
            String nacosServiceName = entry.getKey();
            List<Instance> needRegisterInstances = entry.getValue();
            if (needRegisterInstances.get(0).isEphemeral()) {
                // 批量注册
                log.debug("将源集群指定服务的临时实例全量同步到目标集群的{}: {}", nacosServiceName, taskDO);
                destNamingService.batchRegisterInstance(nacosServiceName, groupName, needRegisterInstances);
            } else {
                for (Instance instance : needRegisterInstances) {
                    log.debug("从源集群同步指定服务到目标集群的{}：{}", nacosServiceName, instance);
                    destNamingService.registerInstance(nacosServiceName, groupName, instance);
                }
            }
        }
    }
    
    @Override
    public boolean delete(TaskDO taskDO) {
        try {
            String serviceName = taskDO.getServiceName();
            String groupName = getGroupNameOrDefault(taskDO.getGroupName());
            CloseableUtils.closeQuietly(treeCacheMap.get(taskDO.getTaskId()));
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId());
            if (!ALL_SERVICE_NAME_PATTERN.equals(serviceName)) {
                String nacosServiceName = nacosServiceNameMap.get(serviceName);
                if (nacosServiceName == null) {
                    return true;
                }
                List<Instance> allInstances = destNamingService.getAllInstances(
                        nacosServiceName, groupName, new ArrayList<>(), true);
                List<Instance> needDeregisterInstances = new ArrayList<>();
                for (Instance instance : allInstances) {
                    if (needDelete(instance.getMetadata(), taskDO)) {
                        NacosSyncToNacosServiceImpl.removeUnwantedAttrsForNacosRedo(instance);
                        log.debug("需要反注册的实例: {}", instance);
                        needDeregisterInstances.add(instance);
                    }
                }
                if (CollectionUtils.isEmpty(needDeregisterInstances)) {
                    return true;
                }
                NacosSyncToNacosServiceImpl.doDeregisterInstance(taskDO, destNamingService, nacosServiceName,
                        groupName, needDeregisterInstances);
                nacosServiceNameMap.remove(serviceName);
            } else {
                for (Map.Entry<String, String> entry : nacosServiceNameMap.entrySet()) {
                    String nacosServiceName = entry.getValue();
                    List<Instance> allInstances = destNamingService.getAllInstances(nacosServiceName,
                            groupName, new ArrayList<>(), true);
                    List<Instance> needDeregisterInstances = new ArrayList<>();
                    for (Instance instance : allInstances) {
                        if (needDelete(instance.getMetadata(), taskDO)) {
                            NacosSyncToNacosServiceImpl.removeUnwantedAttrsForNacosRedo(instance);
                            log.debug("需要反注册的实例: {}", instance);
                            needDeregisterInstances.add(instance);
                        }
                    }
                    if (CollectionUtils.isEmpty(needDeregisterInstances)) {
                        continue;
                    }
                    NacosSyncToNacosServiceImpl.doDeregisterInstance(taskDO, destNamingService, nacosServiceName,
                            groupName, needDeregisterInstances);
                }
                nacosServiceNameMap.clear();
            }
        } catch (Exception e) {
            log.error("delete task from zookeeper to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            metricsManager.recordError(MetricsStatisticsType.DELETE_ERROR);
            return false;
        }
        return true;
    }
    
    /**
     * fetch the Path cache when the task sync
     */
    protected TreeCache getTreeCache(TaskDO taskDO) {
        return treeCacheMap.computeIfAbsent(taskDO.getTaskId(), (key) -> {
            try {
                TreeCache treeCache = new TreeCache(zookeeperServerHolder.get(taskDO.getSourceClusterId()),
                        DUBBO_ROOT_PATH);
                treeCache.start();
                return treeCache;
            } catch (Exception e) {
                log.error("zookeeper path children cache start failed, taskId:{}", taskDO.getTaskId(), e);
                return null;
            }
        });
        
    }
    
    /**
     * The instance information that needs to be synchronized is matched based on the dubbo version and the grouping
     * name
     */
    protected boolean isMatch(TaskDO taskDO, Map<String, String> queryParam) {
        Predicate<TaskDO> isVersionEq = (task) -> StringUtils.isBlank(taskDO.getVersion()) || StringUtils.equals(
                task.getVersion(), queryParam.get(VERSION_KEY));
        Predicate<TaskDO> isGroupEq = (task) -> StringUtils.isBlank(taskDO.getGroupName()) || StringUtils.equals(
                task.getGroupName(), queryParam.get(GROUP_KEY));
        Predicate<TaskDO> isServiceEq = (task) -> StringUtils.isNotBlank(taskDO.getServiceName()) && StringUtils.equals(
                task.getServiceName(), queryParam.get(INTERFACE_KEY));
        Predicate<TaskDO> isMatchAll = (task) -> StringUtils.isNotBlank(taskDO.getServiceName()) && StringUtils.equals(
                taskDO.getServiceName(), ALL_SERVICE_NAME_PATTERN);
        return isVersionEq.and(isGroupEq).and(isServiceEq).or(isMatchAll).test(taskDO);
    }
    
    /**
     * create Nacos service instance
     *
     * @param queryParam   dubbo metadata
     * @param ipAndPortMap dubbo ip and address
     */
    protected Instance buildSyncInstance(Map<String, String> queryParam, Map<String, String> ipAndPortMap,
            TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(ipAndPortMap.get(INSTANCE_IP_KEY));
        temp.setPort(Integer.parseInt(ipAndPortMap.get(INSTANCE_PORT_KEY)));
        //查询nacos集群实例返回的serviceName含组名前缀，但Nacos2服务端检查批量注册请求serviceName参数时不能包含组名前缀，因此注册实例到目标集群时不再设置serviceName。
        temp.setWeight(Double.parseDouble(queryParam.get(WEIGHT_KEY) == null ? "1.0" : queryParam.get(WEIGHT_KEY)));
        temp.setHealthy(true);
        
        Map<String, String> metaData = new HashMap<>(queryParam);
        metaData.put(PROTOCOL_KEY, ipAndPortMap.get(PROTOCOL_KEY));
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
                skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }
    
    /**
     * cteate Dubbo service name
     *
     * @param serviceName dubbo service name
     * @param queryParam  dubbo metadata
     */
    protected String getServiceNameFromCache(String serviceName, Map<String, String> queryParam) {
        return nacosServiceNameMap.computeIfAbsent(serviceName, (key) -> createServiceName(queryParam));
    }

    public static boolean instanceEquals(Instance ins1, Instance ins2) {
        return (ins1.getIp().equals(ins2.getIp())) && (ins1.getPort() == ins2.getPort());
    }
    
}
