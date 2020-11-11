package com.alibaba.nacossync.extension.impl.extend;

import com.alibaba.nacossync.extension.sharding.ConsistentHashServiceSharding;
import com.alibaba.nacossync.extension.sharding.ServiceSharding;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.TreeSet;

/**
 * Created by maj on 2020/10/30.
 */
@Service
@Slf4j
public class ZookeeperSyncToNacosServiceSharding implements Sharding {

    private static final String SHARDING_KEY_NAME = ZookeeperSyncToNacosServiceSharding.class.getName();

    private volatile String serviceListMd5;

    private volatile boolean serverChange = false;

    @Lazy
    @Resource(type = ConsistentHashServiceSharding.class)
    private ServiceSharding serviceSharding;


    @Override
    public void onServerChange() {
        serverChange = true;
    }

    @Override
    public void start(TaskDO taskDO) {
        serviceSharding.addServerChange(SHARDING_KEY_NAME, this);
    }

    @Override
    public Queue<String> getaAddServices() {
        return serviceSharding.getaAddServices(SHARDING_KEY_NAME);
    }

    @Override
    public Queue<String> getRemoveServices() {
        return serviceSharding.getRemoveServices(SHARDING_KEY_NAME);
    }

    @Override
    public void doSharding(String key, List<String> serviceNames) {
        try {
            if (servicesIschanged(serviceNames) || serverChange) {
                log.info("zk ->nacos reshading start");
                serverChange = false;
                serviceSharding.sharding(SHARDING_KEY_NAME, serviceNames);
            }
        } catch (Exception e) {
            log.error("zk ->nacos reshading faild.", e);
        }
    }

    @Override
    public TreeSet<String> getLocalServices(String key) {
        return serviceSharding.getLoacalServices(SHARDING_KEY_NAME);
    }

    protected boolean servicesIschanged(List<String> serviceNames) throws Exception {//zk区分不了是service变化还是instance变化
        Collections.sort(serviceNames);
        String md5 = SkyWalkerUtil.StringToMd5(serviceNames.toString());
        if (!md5.equals(serviceListMd5)) {
            serviceListMd5 = md5;
            return true;
        }
        return false;
    }

    @Override
    public void stop(TaskDO taskDO) {

    }
}
