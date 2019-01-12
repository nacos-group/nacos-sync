package com.alibaba.nacossync.utils;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.nacossync.util.SkyWalkerUtil;

/**
 * @author NacosSync
 * @version $Id: SkyWalkerUtilTest.java, v 0.1 2018-09-26 下午3:47 NacosSync Exp $$
 */
public class SkyWalkerUtilTest {


    @Test
    public void skyWalkerUtilTest() throws Exception {

        String ip = SkyWalkerUtil.getLocalIp();
        Assert.assertNotEquals("127.0.0.1", ip);
    }

}
