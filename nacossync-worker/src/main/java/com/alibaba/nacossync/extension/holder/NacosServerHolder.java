package com.alibaba.nacossync.extension.holder;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Properties;

/**
 * @author paderlol
 * @date: 2018-12-24 21:48
 */
@Service
@Slf4j
public class NacosServerHolder extends AbstractServerHolder<NamingService> {

    @Override
    NamingService createServer(String serverAddress, String namespace) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverAddress);
        properties.setProperty(PropertyKeyConst.NAMESPACE, namespace);
        return NamingFactory.createNamingService(properties);
    }
}
