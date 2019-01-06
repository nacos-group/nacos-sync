package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.CloseableUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static com.alibaba.nacossync.util.DubboConstants.*;
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

    /**
     * 存放zk监听缓存 格式taskId -> PathChildrenCache实例
     */
    private Map<String, PathChildrenCache> pathChildrenCacheMap = new ConcurrentHashMap<>();
    /**
     * 存放zk监听缓存
     */
    private Map<String, String> nacosServiceNameMap = new ConcurrentHashMap<>();

    private final ZookeeperServerHolder zookeeperServerHolder;

    private final NacosServerHolder nacosServerHolder;

    private final SkyWalkerCacheServices skyWalkerCacheServices;

    @Autowired
    public ZookeeperSyncToNacosServiceImpl(ZookeeperServerHolder zookeeperServerHolder, NacosServerHolder nacosServerHolder, SkyWalkerCacheServices skyWalkerCacheServices) {
        this.zookeeperServerHolder = zookeeperServerHolder;
        this.nacosServerHolder = nacosServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            if (pathChildrenCacheMap.containsKey(taskDO.getTaskId())) {
                return true;
            }

            PathChildrenCache pathChildrenCache = getPathCache(taskDO);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            List<ChildData> currentData = pathChildrenCache.getCurrentData();
            for (ChildData childData : currentData) {
                String path = childData.getPath();
                Map<String, String> queryParam = parseQueryString(childData.getPath());
                if (isMatch(taskDO, queryParam) && needSync(queryParam)) {
                    Map<String, String> ipAndPortParam = parseIpAndPortString(path);
                    Instance instance = buildSyncInstance(queryParam, ipAndPortParam, taskDO);
                    destNamingService.registerInstance(getServiceNameFromCache(taskDO.getTaskId(), queryParam),
                        instance);
                }
            }
            Objects.requireNonNull(pathChildrenCache).getListenable().addListener((client, event) -> {
                try {

                    String path = event.getData().getPath();
                    Map<String, String> queryParam = parseQueryString(path);

                    if (isMatch(taskDO, queryParam) && needSync(queryParam)) {
                        Map<String, String> ipAndPortParam = parseIpAndPortString(path);
                        Instance instance = buildSyncInstance(queryParam, ipAndPortParam, taskDO);
                        switch (event.getType()) {
                            case CHILD_ADDED:
                                destNamingService.registerInstance(
                                    getServiceNameFromCache(taskDO.getTaskId(), queryParam), instance);
                                break;
                            case CHILD_UPDATED:

                                destNamingService.registerInstance(
                                    getServiceNameFromCache(taskDO.getTaskId(), queryParam), instance);
                                break;
                            case CHILD_REMOVED:

                                destNamingService.deregisterInstance(
                                    getServiceNameFromCache(taskDO.getTaskId(), queryParam),
                                    ipAndPortParam.get(INSTANCE_IP_KEY),
                                    Integer.parseInt(ipAndPortParam.get(INSTANCE_PORT_KEY)));

                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    log.error("event process from zookeeper to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
                }

            });
        } catch (Exception e) {
            log.error("sync task from zookeeper to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        try {

            CloseableUtils.closeQuietly(pathChildrenCacheMap.get(taskDO.getTaskId()));
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            List<Instance> allInstances =
                destNamingService.getAllInstances(nacosServiceNameMap.get(taskDO.getTaskId()));
            for (Instance instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO)) {
                    destNamingService.deregisterInstance(instance.getServiceName(), instance.getIp(),
                        instance.getPort());
                }
                nacosServiceNameMap.remove(taskDO.getTaskId());

            }

        } catch (Exception e) {
            log.error("delete task from zookeeper to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    /**
     * 获取zk path child 监听缓存类
     * 
     * @param taskDO
     * @return
     */
    private PathChildrenCache getPathCache(TaskDO taskDO) {
        return pathChildrenCacheMap.computeIfAbsent(taskDO.getTaskId(), (key) -> {
            try {
                PathChildrenCache pathChildrenCache =
                    new PathChildrenCache(zookeeperServerHolder.get(taskDO.getSourceClusterId(), ""),
                        monitorPath(taskDO.getServiceName()), false);
                pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                return pathChildrenCache;
            } catch (Exception e) {
                log.error("zookeeper path children cache start failed, taskId:{}", taskDO.getTaskId(), e);
                return null;
            }
        });

    }

    /**
     * 创建zk监听路径 /dubbo/serviceName/providers
     * 
     * @param serviceName 服务名
     * @return
     */
    private static String monitorPath(String serviceName) {
        return String.format(DUBBO_PATH_FORMAT, serviceName);
    }

    /**
     * 根据dubbo 版本和分组名匹配是否是需要同步的实例信息
     * 
     * @param taskDO
     * @param queryParam
     * @return
     */
    private boolean isMatch(TaskDO taskDO, Map<String, String> queryParam) {
        Predicate<TaskDO> isVersionEq =
            (task) -> task.getVersion() == null || StringUtils.equals(task.getVersion(), queryParam.get(VERSION_KEY));
        Predicate<TaskDO> isGroupEq =
            (task) -> task.getGroupName() == null || StringUtils.equals(task.getGroupName(), queryParam.get(GROUP_KEY));
        return isVersionEq.and(isGroupEq).test(taskDO);
    }

    /**
     * 构建Nacos 注册服务实例
     * 
     * @param queryParam
     * @param ipAndPortMap
     * @param taskDO
     * @return
     */
    private Instance buildSyncInstance(Map<String, String> queryParam, Map<String, String> ipAndPortMap,
        TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(ipAndPortMap.get(INSTANCE_IP_KEY));
        temp.setPort(Integer.parseInt(ipAndPortMap.get(INSTANCE_PORT_KEY)));
        temp.setServiceName(getServiceNameFromCache(taskDO.getTaskId(), queryParam));
        temp.setWeight(Double.valueOf(queryParam.get(WEIGHT_KEY) == null ? "1.0" : queryParam.get(WEIGHT_KEY)));
        temp.setHealthy(true);

        Map<String, String> metaData = new HashMap<>();
        metaData.putAll(queryParam);
        metaData.put(PROTOCOL_KEY, ipAndPortMap.get(PROTOCOL_KEY));
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }

    /**
     * 构建Dubbo 格式服务名
     * 
     * @param taskId 任务ID
     * @param queryParam dubbo 元数据
     * @return
     */
    private String getServiceNameFromCache(String taskId, Map<String, String> queryParam) {
        return nacosServiceNameMap.computeIfAbsent(taskId, (key) -> Joiner.on(":").skipNulls().join(CATALOG_KEY,
            queryParam.get(INTERFACE_KEY), queryParam.get(VERSION_KEY), queryParam.get(GROUP_KEY)));
    }

}
