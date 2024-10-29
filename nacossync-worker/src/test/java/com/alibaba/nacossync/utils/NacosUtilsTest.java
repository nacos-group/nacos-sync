package com.alibaba.nacossync.utils;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacossync.util.NacosUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NacosUtilsTest {
    
    /**
     * Test getGroupNameOrDefault with a non-blank group name.
     */
    @Test
    public void testGetGroupNameOrDefault_withNonBlankGroupName() {
        String groupName = "testGroup";
        String result = NacosUtils.getGroupNameOrDefault(groupName);
        assertEquals("testGroup", result);
    }
    
    /**
     * Test getGroupNameOrDefault with a blank group name.
     */
    @Test
    public void testGetGroupNameOrDefault_withBlankGroupName() {
        String groupName = " ";
        String result = NacosUtils.getGroupNameOrDefault(groupName);
        assertEquals(Constants.DEFAULT_GROUP, result);
    }
    
    /**
     * Test getGroupNameOrDefault with a null group name.
     */
    @Test
    public void testGetGroupNameOrDefault_withNullGroupName() {
        String result = NacosUtils.getGroupNameOrDefault(null);
        assertEquals(Constants.DEFAULT_GROUP, result);
    }
}
