package com.alibaba.nacossync.utils;

import com.alibaba.nacossync.util.DubboConstants;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.nacossync.util.DubboConstants.GROUP_KEY;
import static com.alibaba.nacossync.util.DubboConstants.INTERFACE_KEY;
import static com.alibaba.nacossync.util.DubboConstants.RELEASE_KEY;
import static com.alibaba.nacossync.util.DubboConstants.VERSION_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DubboConstantsTest {
    @Test
    public void testCreateServiceName_withValidParameters() {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(INTERFACE_KEY, "com.example.Service");
        queryParams.put(VERSION_KEY, "1.0.0");
        queryParams.put(GROUP_KEY, "testGroup");
        
        String expectedServiceName = "providers:com.example.Service:1.0.0:testGroup";
        String actualServiceName = DubboConstants.createServiceName(queryParams);
        assertEquals(expectedServiceName, actualServiceName);
    }
    
    @Test
    public void testCreateServiceName_withBlankGroup() {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(INTERFACE_KEY, "com.example.Service");
        queryParams.put(VERSION_KEY, "1.0.0");
        queryParams.put(RELEASE_KEY, "2.7.3");
        
        String expectedServiceName = "providers:com.example.Service:1.0.0:";
        String actualServiceName = DubboConstants.createServiceName(queryParams);
        assertEquals(expectedServiceName, actualServiceName);
    }
    
    @Test
    public void testCreateServiceName_withNoGroupOrRelease() {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(INTERFACE_KEY, "com.example.Service");
        queryParams.put(VERSION_KEY, "1.0.0");
        
        String expectedServiceName = "providers:com.example.Service:1.0.0";
        String actualServiceName = DubboConstants.createServiceName(queryParams);
        assertEquals(expectedServiceName, actualServiceName);
    }
    
    @Test
    public void testCreateServiceName_withComplexRelease() {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(INTERFACE_KEY, "com.example.Service");
        queryParams.put(VERSION_KEY, "1.0.0");
        queryParams.put(RELEASE_KEY, "2.8.1");
        
        String expectedServiceName = "providers:com.example.Service:1.0.0:";
        String actualServiceName = DubboConstants.createServiceName(queryParams);
        assertEquals(expectedServiceName, actualServiceName);
    }
}
