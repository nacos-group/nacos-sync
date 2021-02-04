package com.alibaba.nacossync.extension.impl.extend;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.client.naming.utils.NetUtils;
import com.alibaba.nacossync.constant.ShardingLogTypeEnum;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.impl.NacosSyncToZookeeperServiceImpl;
import com.alibaba.nacossync.extension.sharding.ConsistentHashServiceSharding;
import com.alibaba.nacossync.extension.sharding.ServiceSharding;
import com.alibaba.nacossync.pojo.ShardingLog;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.DubboConstants;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by maj on 2020/10/29.
 */
@Service
@Slf4j
public class NacosSyncToZookeeperServicesSharding implements Sharding {

    @Autowired
    private NacosServerHolder nacosServerHolder;

    public static final int DEFAULT_SERVICE_PAGENO = 1;

    public static final int DEFAULT_SERVICE_PAGE_SIZE = 10000;

    private volatile String serviceListMd5;

    @Autowired
    private SyncManagerService syncManagerService;

    @Lazy
    @Resource(type = ConsistentHashServiceSharding.class)
    private ServiceSharding serviceSharding;

    private static final String SHARDING_KEY_NAME = NacosSyncToZookeeperServicesSharding.class.getName();

    private static final long DEFAULT_SERVICES_CHANGE_THREAD_DELAY = 10;

    private static final long DEFAULT_SERVICES_CHANGE_THREAD_INTERVAL = 5;

    //add cache taskDO
    private volatile Map<String, TaskDO> taskDOMap = new ConcurrentHashMap<>();

    @Value("${server.port}")
    private String serverPort;


    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("com.dmall.dmnacos.sync.getServiceName");
            return thread;
        }
    });

    protected List<String> servicesIschanged(TaskDO taskDO) throws Exception {
        NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getNameSpace());
        List<String> serviceNames = sourceNamingService.getServicesOfServer(DEFAULT_SERVICE_PAGENO, DEFAULT_SERVICE_PAGE_SIZE, SkyWalkerUtil.getGroupName(taskDO.getGroupName())).getData();
        Collections.sort(serviceNames);
        String md5 = SkyWalkerUtil.StringToMd5(serviceNames.toString());
        if (!md5.equals(serviceListMd5)) {
            serviceListMd5 = md5;
            return serviceNames;
        }
        return null;
    }

    protected synchronized void reSubscribeService(TaskDO taskDO, List<String> serviceNames) {
        if (taskDO == null) return;
        try {
            NamingService sourceNamingService =
                    nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getNameSpace());
            if (serviceNames == null) {
                serviceNames = sourceNamingService.getServicesOfServer(DEFAULT_SERVICE_PAGENO, DEFAULT_SERVICE_PAGE_SIZE, taskDO.getGroupName()).getData();//如果使用同一个groupName暂时没问题，如果配置了多个group，需要升级sdk1.4+,支持按照*的group查询
            }
            serviceSharding.sharding(SHARDING_KEY_NAME, serviceNames);
        } catch (Exception e) {
            log.error("reSubscribe faild,task id:{}", taskDO.getId(), e);
        }
        if (!serviceSharding.getChangeServices(SHARDING_KEY_NAME).isEmpty()) {
            try {
                synChangeServices();
            } catch (Exception e) {
                log.error("reSubscribe -->delete service faild,task id:{}", taskDO.getId(), e);
            }
        }
    }

    @Override
    public void start(TaskDO taskDO) {
        taskDOMap.putIfAbsent(taskDO.getServiceName(), taskDO);
        if (!serviceSharding.addServerChange(SHARDING_KEY_NAME, this)) {
            return;
        }
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    List<String> changeList = servicesIschanged(taskDO);
                    if (changeList != null) {
                        reSubscribeService(taskDO, changeList);
                    }
                } catch (Exception e) {
                    log.error("schedule reSubscribe service thread faild ,task id :", taskDO.getId(), e);
                }
            }
        }, DEFAULT_SERVICES_CHANGE_THREAD_DELAY, DEFAULT_SERVICES_CHANGE_THREAD_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void onServerChange() {
        if (taskDOMap.size() > 0) {
            for (TaskDO taskDO : taskDOMap.values()) {//任意取一个taskDo
                reSubscribeService(taskDO, null);
                return;
            }
        }
    }

    @Override
    public Queue<ShardingLog> getChangeService() {
        return serviceSharding.getChangeServices(SHARDING_KEY_NAME);
    }

    @Override
    public void doSharding(String key, List<String> serviceNames) {
        serviceSharding.sharding(SHARDING_KEY_NAME, serviceNames);
    }

    @Override
    public TreeSet<String> getLocalServices() {
        return serviceSharding.getLocalServices(SHARDING_KEY_NAME);
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


    private void synAddServices(String serviceName) {
        if (taskDOMap.containsKey(DubboConstants.ALL_SERVICE_NAME_PATTERN)) {//如果有配置为* 的 则不用处理单独配置serviceName的task
            TaskDO taskDO = SkyWalkerUtil.buildNewTaskDo(taskDOMap.get(DubboConstants.ALL_SERVICE_NAME_PATTERN), serviceName);
            ((NacosSyncToZookeeperServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).addSynService(taskDO);
            return;
        }
        if (taskDOMap.containsKey(serviceName)) {//如果有配置变更的serviceName，而且sharding到本server则处理
            TaskDO taskDO = SkyWalkerUtil.buildNewTaskDo(taskDOMap.get(serviceName), serviceName);
            ((NacosSyncToZookeeperServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).addSynService(taskDO);
        }

    }

    private void synRemoveServices(String serviceName) {
        if (taskDOMap.containsKey(DubboConstants.ALL_SERVICE_NAME_PATTERN)) {
            TaskDO taskDO = SkyWalkerUtil.buildNewTaskDo(taskDOMap.get(DubboConstants.ALL_SERVICE_NAME_PATTERN), serviceName);
            ((NacosSyncToZookeeperServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).removeSyncServices(taskDO);
            return;
        }
        if (taskDOMap.containsKey(serviceName)) {
            TaskDO taskDO = SkyWalkerUtil.buildNewTaskDo(taskDOMap.get(serviceName), serviceName);
            ((NacosSyncToZookeeperServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).removeSyncServices(taskDO);
        }

    }

    private void synChangeServices() {
        while (!serviceSharding.getChangeServices(SHARDING_KEY_NAME).isEmpty()) {
            ShardingLog shardingLog = serviceSharding.getChangeServices(SHARDING_KEY_NAME).poll();
            if (shardingLog.getType().equals(ShardingLogTypeEnum.ADD.getType())) {
                synAddServices(shardingLog.getServiceName());
            }
            if (shardingLog.getType().equals(ShardingLogTypeEnum.DELETE.getType())) {
                synRemoveServices(shardingLog.getServiceName());
            }
        }
    }

    @Override
    public void stop(TaskDO taskDO) {
        if (taskDOMap.containsKey(taskDO.getServiceName())) {
            taskDOMap.remove(taskDO.getServiceName());
        }
    }

    @Override
    public void reShardingIfNeed() {

    }
}
