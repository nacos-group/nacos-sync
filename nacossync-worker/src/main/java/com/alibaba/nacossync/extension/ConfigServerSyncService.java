package com.alibaba.nacossync.extension;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.google.common.collect.ImmutableMap;
import com.taobao.config.client.LocalAttribute;
import com.taobao.config.client.Subscriber;
import com.taobao.config.client.SubscriberDataObserver;
import com.taobao.config.client.SubscriberRegistrar;
import com.taobao.config.client.SubscriberRegistration;

import lombok.extern.slf4j.Slf4j;

/**
 * @author yangyshdan
 * @version $Id: ConfigServerSyncManagerService.java, v 0.1 2018-11-12 下午5:17 NacosSync Exp $$
 */

@Slf4j
@Service
public class ConfigServerSyncService implements SyncService {

	

    private SyncManagerService syncManagerService;
    
    @Autowired
    protected ConfigServerSyncService(SyncManagerService service)
    {
    	this.syncManagerService = service;
    	syncManagerService.register(ClusterTypeEnum.CS, this);
    }

    public boolean sync(TaskDO taskDO) 
    {
        String serviceName = taskDO.getServiceName();
        String groupName = taskDO.getGroupName();

        SubscriberRegistration subscriberRegistration = getSubscriberRegistration(taskDO,
            serviceName, groupName);

        Subscriber subscriber = SubscriberRegistrar.register(subscriberRegistration);
        //订阅数据
        subscriber.setDataObserver(new SubscriberDataObserver() {
            @Override
            public void handleData(String dataId, List<Object> list) {

                log.info("cs callback data:" + dataId + list);

                String sourceClusterId = taskDO.getSourceClusterId();
                String destClusterId = taskDO.getDestClusterId();
                Set<String> newData = new HashSet<>();
                final NamingService namingService;

                try {

                    namingService = NacosFactory.createNamingService(syncManagerService.skyWalkerCacheServices
                        .getClusterConnectKey(taskDO.getDestClusterId()));
                    //先把ip注册上去
                    for (Object data : list) {
                        //根据cs的数据构造1个instance
                        Instance instance = getInstance(groupName, sourceClusterId, destClusterId,
                            (String) data);
                        try {

                            namingService.registerInstance(dataId, instance);
                        } catch (NacosException e) {

                            log.info("namingService.registerInstance:" + instance);
                        }

                        newData.add(instance.getIp() + ":" + instance.getPort());
                    }

                    //删除不存在的ip
                    List<Instance> instances = namingService.getAllInstances(serviceName);

                    for (Instance instance : instances) {

                        if (!newData.contains(instance.getIp() + ":" + instance.getPort())) {

                            namingService.deregisterInstance(serviceName, instance.getIp(),
                                instance.getPort());

                            log.info("namingService.deregisterInstance:" + instance);
                        }
                    }

                } catch (NacosException e) {

                    log.warn("NacosFactory.createNamingService", e);
                }
            }
        });

        this.syncManagerService.skyWalkerCacheServices.addSubscriber(taskDO.getTaskId(), subscriber);

        return true;
    }

    private SubscriberRegistration getSubscriberRegistration(TaskDO taskDO, String serviceName,
                                                             String groupName) {

        SubscriberRegistration subscriberRegistration = this.syncManagerService.skyWalkerCacheServices
            .putIfNotSubscriberRegistration(taskDO.getTaskId(), serviceName);

        subscriberRegistration.setGroup(groupName);
        subscriberRegistration.setLocalAttribute(LocalAttribute.ATTRIBUTE_SERVER,
        		this.syncManagerService.skyWalkerCacheServices.getClusterConnectKey(taskDO.getSourceClusterId()));

        return subscriberRegistration;
    }

    private Instance getInstance(String groupName, String sourceClusterId, String destClusterId,
                                 String data) {

        Instance instance = new Instance();
        //"192.168.1.1:12200?a=1&b=2"
        String[] datas = StringUtils.split(data, "?");
        String[] values = StringUtils.split(datas[0], ":");
        instance.setIp(values[0]);
        instance.setPort(Integer.valueOf(values[1]));

        instance.setMetadata(ImmutableMap.<String, String> builder()
            .put(SkyWalkerConstants.DEST_CLUSTERID_KEY, destClusterId)
            .put(SkyWalkerConstants.GROUP_NAME, groupName)
            .put(SkyWalkerConstants.SYNC_SOURCE_KEY, ClusterTypeEnum.CS.getCode())
            .put(SkyWalkerConstants.SOURCE_CLUSTERID_KEY, sourceClusterId).build());

        return instance;
    }
    
    

	@Override
	public boolean delete(TaskDO taskDO) throws NacosException {

        String serviceName = taskDO.getServiceName();

        Subscriber subscriber = this.syncManagerService.skyWalkerCacheServices.getSubscriber(taskDO.getTaskId());

        if (null != subscriber) {

            SubscriberRegistrar.unregister(subscriber);
            log.info("configserver unregister:" + serviceName);
        }

        NamingService namingService = NacosFactory.createNamingService(this.syncManagerService.skyWalkerCacheServices
            .getClusterConnectKey(taskDO.getDestClusterId()));

        try {

            List<Instance> instances = namingService.getAllInstances(serviceName);

            for (Instance instance : instances) {

                namingService.deregisterInstance(serviceName, instance.getIp(), instance.getPort());
            }

        } catch (Exception e) {

            log.warn("namingService.getAllInstances(serviceName) error", e);
        }

        return true;
	}
}
