package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.EurekaServerHolder;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.transport.EurekaHttpClient;
import com.netflix.discovery.shared.transport.EurekaHttpResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * eureka
 * 
 * @author paderlol
 * @date: 2018-12-31 16:25
 */
@Slf4j
@NacosSyncService(clusterType = ClusterTypeEnum.EUREKA)
public class EurekaSyncServiceImpl implements com.alibaba.nacossync.extension.SyncService {

    @Autowired
    private EurekaServerHolder eurekaServerHolder;
    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;

    @Autowired
    private NacosServerHolder nacosServerHolder;

    @Override
    public boolean delete(TaskDO taskDO) {

        try {
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            List<Instance> allInstances = destNamingService.getAllInstances(taskDO.getServiceName());
            for (Instance instance : allInstances) {
                if (needDelete(instance.getMetadata(), taskDO)) {
                    destNamingService.deregisterInstance(taskDO.getServiceName(), instance.getIp(), instance.getPort());
                }
            }

        } catch (Exception e) {
            log.error("delete task from eureka to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        try {
            EurekaHttpClient eurekaHttpClient = eurekaServerHolder.get(taskDO.getSourceClusterId(), null);
            NamingService destNamingService = nacosServerHolder.get(taskDO.getDestClusterId(), null);
            EurekaHttpResponse<Application> eurekaHttpResponse =
                eurekaHttpClient.getApplication(taskDO.getServiceName());
            if (Objects.requireNonNull(HttpStatus.resolve(eurekaHttpResponse.getStatusCode())).is2xxSuccessful()) {
                Application application = eurekaHttpResponse.getEntity();
                for (InstanceInfo instanceInfo : application.getInstances()) {
                    if (InstanceInfo.InstanceStatus.UP.equals(instanceInfo.getStatus())) {
                        destNamingService.registerInstance(taskDO.getServiceName(),
                            buildSyncInstance(instanceInfo, taskDO));
                    } else {
                        destNamingService.deregisterInstance(instanceInfo.getAppName(), instanceInfo.getIPAddr(),
                            instanceInfo.getPort());
                    }
                }
            } else {
                throw new RuntimeException("trying to connect to the server failed");
            }
        } catch (Exception e) {
            log.error("sync task from eureka to nacos was failed, taskId:{}", taskDO.getTaskId(), e);
            return false;
        }
        return true;
    }

    /**
     * 判断当前实例数据是否源集群信息是一致的， 一致才会进行删除
     *
     * @param destMetaData
     * @param taskDO
     * @return
     */
    private boolean needDelete(Map<String, String> destMetaData, TaskDO taskDO) {
        return StringUtils.equals(destMetaData.get(SkyWalkerConstants.SOURCE_CLUSTERID_KEY),
            taskDO.getSourceClusterId());
    }

    private Instance buildSyncInstance(InstanceInfo instance, TaskDO taskDO) {
        Instance temp = new Instance();
        temp.setIp(instance.getIPAddr());
        temp.setPort(instance.getPort());
        temp.setServiceName(instance.getAppName());
        temp.setHealthy(true);

        Map<String, String> metaData = new HashMap<>();
        metaData.putAll(instance.getMetadata());
        metaData.put(SkyWalkerConstants.DEST_CLUSTERID_KEY, taskDO.getDestClusterId());
        metaData.put(SkyWalkerConstants.SYNC_SOURCE_KEY,
            skyWalkerCacheServices.getClusterType(taskDO.getSourceClusterId()).getCode());
        metaData.put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, taskDO.getSourceClusterId());
        temp.setMetadata(metaData);
        return temp;
    }

    public static void main(String[] args) throws MalformedURLException {
        System.out.println(new URL("127.0.0.1:18033/eureka").getProtocol());
    }
}
