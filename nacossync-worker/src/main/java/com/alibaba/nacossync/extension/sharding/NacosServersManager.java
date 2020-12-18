package com.alibaba.nacossync.extension.sharding;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.client.naming.utils.NetUtils;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.util.StringUtils;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Created by maj on 2020/10/27.
 */
@Service
@Slf4j
@Lazy
public class NacosServersManager implements ServersManager<EventListener>, InitializingBean {

    private NamingService namingService;

    @Value("${server.port}")
    private String serverPort;

    @Autowired
    private SkyWalkerCacheServices skyWalkerCacheServices;

    //不动现有业务表和逻辑基础上，指定一个固定的key--"SHARDINGKEY" 生成md5值 作为默认连接的nacos路径
    private static final String DEFAULT_SHARDING_NACOS_KEY = "3ac01a8c7501f121ab01efb920aa4764";

    private static final String DEFAULT_SHARDING_NACOS_NAMESPACES = "public";

    private static final String DEFAULT_SHARDING_NACOS_GOURPID = "shadinggroup";

    private static final String DEFAULT_SHARDING_NACOS_SERVICENAME = "com.dmall.sharding";

    @Value("${sharding.nacos.url}")
    private String shardingNacosUrl;

    @Value("${sharding.nacos.namespace}")
    private String shardingNacosnameSpace;

    @Value("${sharding.nacos.groupname}")
    private String shardingNacosGroupName;

    @Value("${sharding.nacos.servicename}")
    private String shardingNacosServiceName;

    @Override
    public List<String> getServers() throws Exception {
        List<Instance> instanceList = namingService.getAllInstances(DEFAULT_SHARDING_NACOS_SERVICENAME, DEFAULT_SHARDING_NACOS_GOURPID);
        List<String> serverList = new LinkedList<String>();
        for (Instance instance : instanceList) {
            serverList.add(instance.getIp() + ":" + instance.getPort());
        }
        return serverList;
    }

    @Override
    public void subscribeServers(EventListener listener) throws Exception {
        namingService.subscribe(DEFAULT_SHARDING_NACOS_SERVICENAME, DEFAULT_SHARDING_NACOS_GOURPID, listener);
    }

    @Override
    public void register(String ip, int port) throws Exception {
        namingService.registerInstance(DEFAULT_SHARDING_NACOS_SERVICENAME, DEFAULT_SHARDING_NACOS_GOURPID, ip, port);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            log.info("start init nacos servers.");
            if (StringUtils.isEmpty(shardingNacosUrl)) {
                shardingNacosUrl = DEFAULT_SHARDING_NACOS_KEY;
            }
            Properties properties = new Properties();
            properties.setProperty(PropertyKeyConst.SERVER_ADDR, getNacosUrl());
            properties.setProperty(PropertyKeyConst.NAMESPACE, StringUtils.isEmpty(shardingNacosnameSpace) ? DEFAULT_SHARDING_NACOS_NAMESPACES : shardingNacosnameSpace);
            namingService = NamingFactory.createNamingService(properties);
            register(NetUtils.localIP(), Integer.parseInt(serverPort));
            log.info("init nacos servers sucess.");
        } catch (Exception e) {
            log.info("init nacos faild .", e);
        }
    }

    private String getNacosUrl() {
        if (!StringUtils.isIPV4AndPorts(shardingNacosUrl, ",")) {
            List<String> allClusterConnectKey = skyWalkerCacheServices
                    .getAllClusterConnectKey(StringUtils.isEmpty(shardingNacosUrl) ? DEFAULT_SHARDING_NACOS_KEY : shardingNacosUrl);
            return Joiner.on(",").join(allClusterConnectKey);
        }
        return shardingNacosUrl;
    }
}
