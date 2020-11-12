package com.alibaba.nacossync.extension.impl.extend;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.client.naming.utils.NetUtils;
import com.alibaba.nacossync.extension.SyncManagerService;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.extension.impl.NacosSyncToZookeeperServiceImpl;
import com.alibaba.nacossync.extension.sharding.ConsistentHashServiceSharding;
import com.alibaba.nacossync.extension.sharding.ServiceSharding;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.DubboConstants;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
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

    protected boolean servicesIschanged(TaskDO taskDO) throws Exception {
        NamingService sourceNamingService =
                nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getNameSpace());
        List<String> serviceNames = sourceNamingService.getServicesOfServer(DEFAULT_SERVICE_PAGENO, DEFAULT_SERVICE_PAGE_SIZE, SkyWalkerUtil.getGroupName(taskDO.getGroupName())).getData();
        Collections.sort(serviceNames);
        String md5 = SkyWalkerUtil.StringToMd5(serviceNames.toString());
        if (!md5.equals(serviceListMd5)) {
            serviceListMd5 = md5;
            return true;
        }
        return false;
    }

    protected void reSubscribeService(TaskDO taskDO) {
        if (taskDO == null) return;
        try {
            NamingService sourceNamingService =
                    nacosServerHolder.get(taskDO.getSourceClusterId(), taskDO.getNameSpace());
            List<String> serviceNames = sourceNamingService.getServicesOfServer(DEFAULT_SERVICE_PAGENO, DEFAULT_SERVICE_PAGE_SIZE, taskDO.getGroupName()).getData();//如果使用同一个groupName暂时没问题，如果配置了多个group，需要升级sdk1.4+,支持按照*的group查询
            serviceSharding.sharding(SHARDING_KEY_NAME, serviceNames);
        } catch (Exception e) {
            log.error("reSubscribe faild,task id:{}", taskDO.getId(), e);
        }
        if (!serviceSharding.getaAddServices(SHARDING_KEY_NAME).isEmpty()) {
            log.info("reSubscribe ,local ip: {},add sharding service:{},current sharding service:{}", NetUtils.localIP() + ":serverPort", serviceSharding.getaAddServices(SHARDING_KEY_NAME).toArray(), serviceSharding.getLoacalServices(SHARDING_KEY_NAME).toString());
            try {
                synAddServices();
            } catch (Exception e) {
                log.error("reSubscribe -->sync change service faild,task id:{}", taskDO.getId(), e);
            }
        }
        if (!serviceSharding.getRemoveServices(SHARDING_KEY_NAME).isEmpty()) {
            log.info("reSubscribe ,local ip: {},delete sharding service:{},current sharding service:{}", NetUtils.localIP() + ":serverPort", serviceSharding.getRemoveServices(SHARDING_KEY_NAME).toArray(), serviceSharding.getLoacalServices(SHARDING_KEY_NAME).toString());
            try {
                synRemoveServices();
            } catch (Exception e) {
                log.error("reSubscribe -->delete service faild,task id:{}", taskDO.getId(), e);
            }
        }
    }

    //暂时不支持多source_cluster_id，多nammespace维度（默认取第一个task的source_cluster_id和namespace）
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
                    if (servicesIschanged(taskDO)) {
                        reSubscribeService(taskDO);
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
                reSubscribeService(taskDO);
                return;
            }

        }
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
        serviceSharding.sharding(SHARDING_KEY_NAME, serviceNames);
    }

    @Override
    public TreeSet<String> getLocalServices(String key) {
        return serviceSharding.getLoacalServices(key);
    }

    private void synAddServices() {
        while (!serviceSharding.getaAddServices(SHARDING_KEY_NAME).isEmpty()) {
            String serviceName = serviceSharding.getaAddServices(SHARDING_KEY_NAME).poll();
            if (taskDOMap.containsKey(DubboConstants.ALL_SERVICE_NAME_PATTERN)) {//如果有配置为* 的 则不用处理单独配置serviceName的task
                TaskDO taskDO = buildNewTaskDo(taskDOMap.get(DubboConstants.ALL_SERVICE_NAME_PATTERN), serviceName);
                ((NacosSyncToZookeeperServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).addSynService(taskDO);
                log.info("reSubscribe ,{} is add", serviceName);
                continue;
            }
            if (taskDOMap.containsKey(serviceName)) {//如果有配置变更的serviceName，而且sharding到本server则处理
                TaskDO taskDO = buildNewTaskDo(taskDOMap.get(serviceName), serviceName);
                ((NacosSyncToZookeeperServiceImpl) syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId())).addSynService(taskDO);
                log.info("reSubscribe ,{} is add", serviceName);
            }
        }
    }

    private void synRemoveServices() {
        while (!serviceSharding.getRemoveServices(SHARDING_KEY_NAME).isEmpty()) {
            String serviceName = serviceSharding.getRemoveServices(SHARDING_KEY_NAME).poll();
            if (taskDOMap.containsKey(DubboConstants.ALL_SERVICE_NAME_PATTERN)) {//如果有配置为* 的 则不用处理单独配置serviceName的task
                TaskDO taskDO = buildNewTaskDo(taskDOMap.get(DubboConstants.ALL_SERVICE_NAME_PATTERN), serviceName);
                syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId()).delete(taskDO);
                log.info("reSubscribe ,{} is remove", serviceName);
                continue;
            }
            if (taskDOMap.containsKey(serviceName)) {//如果有配置变更的serviceName，而且sharding到本server则处理
                TaskDO taskDO = buildNewTaskDo(taskDOMap.get(serviceName), serviceName);
                syncManagerService.getSyncService(taskDO.getSourceClusterId(), taskDO.getDestClusterId()).delete(taskDO);
                log.info("reSubscribe ,{} is remove", serviceName);
            }
        }
    }

    @Override
    public void stop(TaskDO taskDO) {
        if (taskDOMap.containsKey(taskDO.getServiceName())) {
            taskDOMap.remove(taskDO.getServiceName());
        }
    }

    private TaskDO buildNewTaskDo(TaskDO taskDO, String serviceName) {
        TaskDO taskDO1 = new TaskDO();
        BeanUtils.copyProperties(taskDO, taskDO1);
        taskDO1.setTaskId(serviceName);//需要一个key替换以前的taskid很多封装维度，暂时使用serviceName
        taskDO1.setServiceName(serviceName);
        return taskDO1;
    }
}
