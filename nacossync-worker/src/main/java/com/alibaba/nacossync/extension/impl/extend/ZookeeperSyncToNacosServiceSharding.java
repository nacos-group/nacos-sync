package com.alibaba.nacossync.extension.impl.extend;

import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacossync.constant.ShardingLogTypeEnum;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.alibaba.nacossync.extension.impl.ZookeeperSyncToNacosServiceImpl;
import com.alibaba.nacossync.extension.sharding.ConsistentHashServiceSharding;
import com.alibaba.nacossync.extension.sharding.ServiceSharding;
import com.alibaba.nacossync.pojo.ShardingLog;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.DubboConstants;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacossync.util.ZookeeperUtils.filterNoProviderPath;

/**
 * Created by maj on 2020/10/30.
 */
@Service
@Slf4j
public class ZookeeperSyncToNacosServiceSharding implements Sharding {

    private static final String SHARDING_KEY_NAME = ZookeeperSyncToNacosServiceSharding.class.getName();

    private volatile String serviceListMd5;

    //add cache taskDO
    private volatile Map<String, TaskDO> taskDOMap = new ConcurrentHashMap<>();

    @Autowired
    private SyncManagerService syncManagerService;

    @Lazy
    @Resource(type = ConsistentHashServiceSharding.class)
    private ServiceSharding serviceSharding;

    private ScheduledExecutorService executor = null;


    @Override
    public void onServerChange() {
        log.info("zk->nacos server is change.....");
        if (taskDOMap.size() > 0) {
            for (TaskDO taskDO : taskDOMap.values()) {//任意取一个taskDo
                List<String> serviceNames = ((ZookeeperSyncToNacosServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).getAllServicesFromZk(taskDO);
                reSubscribeService(filterNoProviderPath(serviceNames));
                return;
            }
        }
    }

    @Override
    public boolean isProcess(TaskDO taskDO, String serviceName) {
        try {
            if (getLocalServices().contains(serviceName)) {
                return true;
            }
        } catch (Exception e) {
            log.error("zk->nacos sharding faild ,taskid:{}", taskDO.getId(), e);
        }
        return false;
    }

    @Override
    public void start(TaskDO taskDO) {
        taskDOMap.putIfAbsent(taskDO.getServiceName(), taskDO);
        serviceSharding.addServerChange(SHARDING_KEY_NAME, this);
        executor = ExecutorFactory
                .newSingleScheduledExecutorService(new NameThreadFactory("com.alibaba.nacossync.zktonacos.resharding.Thread"));

        executor.scheduleWithFixedDelay(() -> {
            if (!serviceSharding.getChangeServices(SHARDING_KEY_NAME).isEmpty()) {
                synChangeServices();
            }
        }, 10, 1, TimeUnit.SECONDS);

    }


    @Override
    public Queue<ShardingLog> getChangeService() {
        return serviceSharding.getChangeServices(SHARDING_KEY_NAME);
    }

    @Override
    public void doSharding(String key, List<String> serviceNames) {
        serviceSharding.sharding(key, serviceNames);
    }

    @Override
    public TreeSet<String> getLocalServices() {
        return serviceSharding.getLocalServices(SHARDING_KEY_NAME);
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

    protected void reSubscribeService(List<String> serviceNames) {
        serviceSharding.sharding(SHARDING_KEY_NAME, serviceNames);
    }

    protected void reSubscribeServiceWithOutChange(List<String> serviceNames) {
        serviceSharding.shardingWithOutAddChange(SHARDING_KEY_NAME, serviceNames);
    }

    private void synChangeServices() {
        while (!serviceSharding.getChangeServices(SHARDING_KEY_NAME).isEmpty()) {
            ShardingLog shardingLog = serviceSharding.getChangeServices(SHARDING_KEY_NAME).poll();
            if (shardingLog.getType().equals(ShardingLogTypeEnum.ADD.getType())) {
                syncAddServices(shardingLog.getServiceName());
            }
            if (shardingLog.getType().equals(ShardingLogTypeEnum.DELETE.getType())) {
                syncRemoveServices(shardingLog.getServiceName());
            }
        }
    }

    private void syncAddServices(String serviceName) {
        if (taskDOMap.containsKey(DubboConstants.ALL_SERVICE_NAME_PATTERN)) {
            TaskDO taskDO = SkyWalkerUtil.buildNewTaskDo(taskDOMap.get(DubboConstants.ALL_SERVICE_NAME_PATTERN), serviceName);
            ((ZookeeperSyncToNacosServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).addSyncService(taskDO);
            return;
        }
        if (taskDOMap.containsKey(serviceName)) {
            TaskDO taskDO = SkyWalkerUtil.buildNewTaskDo(taskDOMap.get(serviceName), serviceName);
            ((ZookeeperSyncToNacosServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).addSyncService(taskDO);
        }

    }

    private void syncRemoveServices(String serviceName) {
        if (taskDOMap.containsKey(DubboConstants.ALL_SERVICE_NAME_PATTERN)) {
            TaskDO taskDO = SkyWalkerUtil.buildNewTaskDo(taskDOMap.get(DubboConstants.ALL_SERVICE_NAME_PATTERN), serviceName);
            ((ZookeeperSyncToNacosServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).removeSyncServices(taskDO);
            return;
        }
        if (taskDOMap.containsKey(serviceName)) {
            TaskDO taskDO = SkyWalkerUtil.buildNewTaskDo(taskDOMap.get(serviceName), serviceName);
            ((ZookeeperSyncToNacosServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).removeSyncServices(taskDO);
        }

    }

    @Override
    public void reShardingIfNeed() {
        try {
            if (taskDOMap.size() == 0) {
                log.error("zk->nacos reshading error,cause by taskDo is null....");
                return;
            }
            TaskDO taskDo = null;
            for (TaskDO taskDO : taskDOMap.values()) {//任意取一个taskDo
                taskDo = taskDO;
                break;
            }
            List<String> serviceNames = ((ZookeeperSyncToNacosServiceImpl) syncManagerService.getSyncService(taskDo.getSourceClusterId(), taskDo.getDestClusterId())).getAllServicesFromZk(taskDo);
            if (servicesIschanged(serviceNames)) {
                reSubscribeServiceWithOutChange(serviceNames);
            }
        } catch (Exception e) {
            log.error("zk ->nacos reshading faild.", e);
        }
    }
}
