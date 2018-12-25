package com.alibaba.nacossync.extension;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.util.HashMap;
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
@Service
public class ZookeeperSyncService implements SyncService {

    private static final String MONITOR_PATH_PATTERN =
        StringUtils.join(new String[] {"/dubbo", "%s", "providers"}, File.separator);
    private Map<String, PathChildrenCache> pathChildrenCacheMap = new ConcurrentHashMap<>();

    @Autowired
    private ZookeeperServerHolder zookeeperServerHolder;

    @Autowired
    private NacosServerHolder nacosServerHolder;

    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            if (pathChildrenCacheMap.containsKey(taskDO.getTaskId())) {
                return true;
            }

            CuratorFramework curatorFramework =
                zookeeperServerHolder.get(taskDO.getSourceClusterId(), taskDO.getGroupName());
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), "");
            PathChildrenCache pathChildrenCache = createPathCache(curatorFramework, taskDO.getServiceName());
            pathChildrenCacheMap.put(taskDO.getTaskId(), pathChildrenCache);
            Objects.requireNonNull(pathChildrenCache).getListenable().addListener((client, event) -> {
                try {

                    String path = event.getData().getPath();
                    Map<String, String> pathMap = parseQueryString(path);
                    Map<String, String> ipAndPortMap = parseIpAndPortString(path);
                    if (isMatch(taskDO, pathMap)) {

                        Instance instance = buildSyncInstance(pathMap, ipAndPortMap, taskDO);
                        switch (event.getType()) {
                            case CHILD_ADDED:
                                destNamingService.registerInstance(composeServiceName(pathMap), instance);
                                break;
                            case CHILD_UPDATED:
                                destNamingService.registerInstance(composeServiceName(pathMap), instance);
                                break;
                            case CHILD_REMOVED:
                                destNamingService.deregisterInstance(composeServiceName(pathMap),
                                    ipAndPortMap.get(INSTANCE_IP_KEY),
                                    Integer.parseInt(ipAndPortMap.get(INSTANCE_IP_KEY)));
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    log.error("event process fail, taskId:{}", taskDO.getTaskId(), e);
                }

            });
        } catch (Exception e) {
            log.error("sync task fail, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        return false;
    }

    @Override
    public ClusterTypeEnum getClusterType() {
        return ClusterTypeEnum.ZK;
    }

    private PathChildrenCache createPathCache(CuratorFramework curatorFramework, String serviceName) throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, monitorPath(serviceName), false);
        pathChildrenCache.start();
        return pathChildrenCache;
    }

    private String monitorPath(String serviceName) {
        return String.format(MONITOR_PATH_PATTERN, serviceName);
    }

    private boolean isMatch(TaskDO taskDO, Map<String, String> pathMap) {
        Predicate<TaskDO> isVersionEq =
            (task) -> task.getVersion() == null || StringUtils.equals(task.getVersion(), pathMap.get(VERSION_KEY));
        Predicate<TaskDO> isGroupEq =
            (task) -> task.getGroupName() == null || StringUtils.equals(task.getGroupName(), pathMap.get(GROUP_KEY));
        return isVersionEq.and(isGroupEq).test(taskDO);
    }

    private Instance buildSyncInstance(Map<String, String> pathMap, Map<String, String> ipAndPortMap, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(ipAndPortMap.get(INSTANCE_IP_KEY));
        temp.setPort(Integer.parseInt(ipAndPortMap.get(INSTANCE_PORT_KEY)));
        // temp.setClusterName(instance.getClusterName());
        temp.setServiceName(composeServiceName(pathMap));
        temp.setEnabled(true);
        temp.setHealthy(true);
        // temp.setWeight(instance.getWeight());

        Map<String, String> metaData = new HashMap<>();
        metaData.putAll(pathMap);
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }

    private static String composeServiceName(Map<String, String> pathMap) {

        return Joiner.on(":").skipNulls().join("provider", pathMap.get(INTERFACE_KEY), pathMap.get(VERSION_KEY),
            pathMap.get(GROUP_KEY));
    }

    public static void main(String[] args) throws UnsupportedEncodingException, MalformedURLException {
        Map<String, String> instanceIpAndPort = parseIpAndPortString(
            "dubbo://172.16.0.12:20880/org.apache.dubbo.demo.DemoService?anyhost=true&application=demo-provider&dubbo=2.0.2&generic=false&group=testGroup&interface=org.apache.dubbo.demo.DemoService&methods=sayHello&pid=26104&revision=1.0.0&side=provider&timestamp=1545740922290&version=1.0.0");
        for (String value : instanceIpAndPort.values()) {
            System.out.println(value);
        }
        Map<String, String> stringStringMap = parseQueryString(URLDecoder.decode(
            "dubbo%3A%2F%2F172.16.0.12%3A20880%2Forg.apache.dubbo.demo.DemoService%3Fanyhost%3Dtrue%26application%3Ddemo-provider%26dubbo%3D2.0.2%26generic%3Dfalse%26group%3DtestGroup%26interface%3Dorg.apache.dubbo.demo.DemoService%26methods%3DsayHello%26pid%3D26104%26revision%3D1.0.0%26side%3Dprovider%26timestamp%3D1545740922290%26version%3D1.0.0",
            "UTF-8"));
        stringStringMap.remove(VERSION_KEY);
        stringStringMap.remove(GROUP_KEY);
        System.out.println(composeServiceName(stringStringMap));

    }

}
