package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.DubboConstants;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacossync.util.DubboConstants.DUBBO_URL_FORMAT;
import static com.alibaba.nacossync.util.DubboConstants.PROTOCOL_KEY;

/**
 * Nacos 同步 Zk 数据
 * 
 * @author paderlol
 * @date 2019年01月06日, 15:08:06
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.NACOS, destinationCluster = ClusterTypeEnum.ZK)
public class NacosSyncToZookeeperServiceImpl implements SyncService {
    private static final String SEPARATOR_CHARS = ":";
    private static final int SEGMENT_LENGTH = 2;
    /**
     * @description The Nacos listener map.
     */
    private Map<String, EventListener> nacosListenerMap = new ConcurrentHashMap<>();
    /**
     * instance backup
     */
    private Map<String, Set<String>> instanceBackupMap = new ConcurrentHashMap<>();

    /**
     * 存放zk监听缓存 格式taskId -> PathChildrenCache实例
     */
    private Map<String, PathChildrenCache> pathChildrenCacheMap = new ConcurrentHashMap<>();
    /**
     * @description The Sky walker cache services.
     */
    private final SkyWalkerCacheServices skyWalkerCacheServices;

    /**
     * @description The Nacos server holder.
     */
    private final NacosServerHolder nacosServerHolder;

    private final ZookeeperServerHolder zookeeperServerHolder;

    @Autowired
    public NacosSyncToZookeeperServiceImpl(SkyWalkerCacheServices skyWalkerCacheServices, NacosServerHolder nacosServerHolder, ZookeeperServerHolder zookeeperServerHolder) {
        this.skyWalkerCacheServices = skyWalkerCacheServices;
        this.nacosServerHolder = nacosServerHolder;
        this.zookeeperServerHolder = zookeeperServerHolder;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        try {

            NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());
            EventListener eventListener = nacosListenerMap.remove(taskDO.getTaskId());
            PathChildrenCache pathChildrenCache = pathChildrenCacheMap.get(taskDO.getTaskId());
            sourceNamingService.unsubscribe(taskDO.getServiceName(), eventListener);
            pathChildrenCache.close();
            Set<String> instanceUrlSet = instanceBackupMap.get(taskDO.getTaskId());
            CuratorFramework client = zookeeperServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());
            for (String instanceUrl : instanceUrlSet) {
                client.delete().quietly().forPath(instanceUrl);
            }
        } catch (Exception e) {
            log.error("delete task from nacos to zk was failed, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());
            CuratorFramework client = zookeeperServerHolder.get(taskDO.getDestClusterId(), taskDO.getGroupName());
            nacosListenerMap.putIfAbsent(taskDO.getTaskId(), event -> {
                if (event instanceof NamingEvent) {
                    try {
                        Set<String> newInstanceUrlSet = new HashSet<>();
                        List<Instance> sourceInstances = sourceNamingService.getAllInstances(taskDO.getServiceName());

                        for (Instance instance : sourceInstances) {
                            if (needSync(instance.getMetadata())) {
                                String instanceUrl = buildSyncInstance(instance, taskDO);
                                if (null == client.checkExists().forPath(instanceUrl)) {
                                    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                                        .forPath(instanceUrl);
                                }
                                newInstanceUrlSet.add(instanceUrl);
                            }
                        }

                        // 获取之前的备份 删除所有
                        Set<String> instanceBackup =
                            instanceBackupMap.getOrDefault(taskDO.getTaskId(), Sets.newHashSet());
                        for (String instanceUrl : instanceBackup) {
                            if (newInstanceUrlSet.contains(instanceUrl)) {
                                continue;
                            }
                            client.delete().quietly().forPath(instanceUrl);
                        }
                        // 替换当前备份为最新备份
                        instanceBackupMap.put(taskDO.getTaskId(), newInstanceUrlSet);
                        getPathCache(taskDO).getListenable().addListener((zkClient, zkEvent) -> {

                            if (zkEvent.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                                List<Instance> allInstances =
                                    sourceNamingService.getAllInstances(taskDO.getServiceName());
                                for (Instance instance : allInstances) {
                                    String instanceUrl = buildSyncInstance(instance, taskDO);
                                    String zkInstancePath = zkEvent.getData().getPath();
                                    if (zkInstancePath.equals(instanceUrl)) {
                                        zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                                            .forPath(zkInstancePath);
                                        break;
                                    }
                                }
                            }
                        });
                    } catch (Exception e) {
                        log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                    }
                }
            });

            sourceNamingService.subscribe(taskDO.getServiceName(), nacosListenerMap.get(taskDO.getTaskId()));
        } catch (Exception e) {
            log.error("sync task from nacos to zk was failed, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    protected String buildSyncInstance(Instance instance, TaskDO taskDO) throws UnsupportedEncodingException {
        Map<String, String> metaData = new HashMap<>();
        metaData.putAll(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        int weight = Integer.parseInt(new DecimalFormat("0").format(Math.ceil(instance.getWeight())));
        metaData.put(DubboConstants.WEIGHT_KEY, Integer.toString(weight));

        String servicePath = getServiceInterface(taskDO.getServiceName());
        String urlParam = Joiner.on("&").withKeyValueSeparator("=").join(metaData);
        String instanceUrl =
            String.format(DUBBO_URL_FORMAT, metaData.get(PROTOCOL_KEY), instance.getIp(), instance.getPort(), urlParam);

        return String.join(File.separator, servicePath, URLEncoder.encode(instanceUrl, "UTF-8"));
    }

    protected  String getServiceInterface(String serviceName) {
        String[] segments = StringUtils.split(serviceName, SEPARATOR_CHARS);
        if (segments.length < SEGMENT_LENGTH) {
            throw new IllegalArgumentException("The length of the split service name must be greater than 2");
        }
        return String.format(DubboConstants.DUBBO_PATH_FORMAT, segments[1]);
    }

    /**
     * 获取zk path child 监听缓存类
     *
     * @param taskDO
     * @return
     */
    protected PathChildrenCache getPathCache(TaskDO taskDO) {
        return pathChildrenCacheMap.computeIfAbsent(taskDO.getTaskId(), (key) -> {
            try {
                PathChildrenCache pathChildrenCache =
                    new PathChildrenCache(zookeeperServerHolder.get(taskDO.getDestClusterId(), ""),
                        getServiceInterface(taskDO.getServiceName()), false);
                pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                return pathChildrenCache;
            } catch (Exception e) {
                log.error("zookeeper path children cache start failed, taskId:{}", taskDO.getTaskId(), e);
                return null;
            }
        });

    }



}
