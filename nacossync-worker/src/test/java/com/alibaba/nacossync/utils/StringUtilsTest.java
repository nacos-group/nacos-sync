package com.alibaba.nacossync.utils;

import com.alibaba.nacossync.util.StringUtils;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;

/**
 * @author paderlol
 * @date: 2019-01-12 17:36
 */

public class StringUtilsTest {

    private static final String TEST_URL =
            "/dubbo/org.apache.dubbo.demo.DemoService/providers/hessian%3A%2F%2F172.16.0.10%3A20880%2Forg.apache.dubbo.demo.DemoService%3Fanyhost%3Dtrue%26application%3Ddemo-provider%26dubbo%3D2.0.2%26generic%3Dfalse%26group%3DtestGroup%26interface%3Dorg.apache.dubbo.demo.DemoService%26methods%3DsayHello%26pid%3D5956%26revision%3D1.0.0%26side%3Dprovider%26timestamp%3D1547285978821%26version%3D1.0.0%26weight%3D1";

    @Test
    public void testParseQueryString() {
        Map<String, String> exceptedMap = Maps.newHashMap();
        exceptedMap.put("side", "provider");
        exceptedMap.put("application", "demo-provider");
        exceptedMap.put("methods", "sayHello");
        exceptedMap.put("dubbo", "2.0.2");
        exceptedMap.put("weight", "1");
        exceptedMap.put("pid", "5956");
        exceptedMap.put("interface", "org.apache.dubbo.demo.DemoService");
        exceptedMap.put("version", "1.0.0");
        exceptedMap.put("group", "testGroup");
        exceptedMap.put("generic", "false");
        exceptedMap.put("revision", "1.0.0");
        exceptedMap.put("timestamp", "1547285978821");
        Map<String, String> actualMap = StringUtils.parseQueryString(TEST_URL);
        Assert.assertThat(actualMap, is(exceptedMap));
    }

    @Test
    public void testParseIpAndPortString() {
        Map<String, String> exceptedMap = Maps.newHashMap();
        exceptedMap.put("protocol", "hessian");
        exceptedMap.put("port", "20880");
        exceptedMap.put("ip", "172.16.0.10");
        Map<String, String> actualMap = StringUtils.parseIpAndPortString(TEST_URL);
        Assert.assertThat(actualMap, is(exceptedMap));
    }

    @Test
    public void testConvertDubboProvidersPath() {
        String exceptedProviderPath = "/dubbo/org.apache.dubbo.demo.DemoService/providers";
        String actualProviderPath = StringUtils
                .convertDubboProvidersPath("org.apache.dubbo.demo.DemoService");
        Assert.assertEquals(exceptedProviderPath, actualProviderPath);
    }

    @Test
    public void testConvertDubboFullPathForZk() {
        String exceptedDubboFullPath = "/dubbo/org.apache.dubbo.demo.DemoService/providers/hessian%3A%2F%2F172.16.0.10%3A20800%2Forg.apache.dubbo.demo.DemoService%3Fside%3Dprovider%26methods%3DsayHello%26dubbo%3D2.0.2%26weight%3D1%26pid%3D5956%26interface%3Dorg.apache.dubbo.demo.DemoService%26version%3D1.0.0%26generic%3Dfalse%26revision%3D1.0.0%26protocol%3Dhessian%26application%3Ddemo-provider%26group%3DtestGroup%26timestamp%3D1547285978821";
        Map<String, String> metaDataMap = Maps.newHashMap();
        metaDataMap.put("side", "provider");
        metaDataMap.put("application", "demo-provider");
        metaDataMap.put("methods", "sayHello");
        metaDataMap.put("dubbo", "2.0.2");
        metaDataMap.put("weight", "1");
        metaDataMap.put("pid", "5956");
        metaDataMap.put("interface", "org.apache.dubbo.demo.DemoService");
        metaDataMap.put("version", "1.0.0");
        metaDataMap.put("group", "testGroup");
        metaDataMap.put("generic", "false");
        metaDataMap.put("revision", "1.0.0");
        metaDataMap.put("timestamp", "1547285978821");
        metaDataMap.put("protocol", "hessian");
        String actualDubboFullPath = StringUtils
                .convertDubboFullPathForZk(metaDataMap,
                        StringUtils.convertDubboProvidersPath("org.apache.dubbo.demo.DemoService"),
                        "172.16.0.10", 20800);
        Assert.assertEquals(exceptedDubboFullPath, actualDubboFullPath);
    }


}
