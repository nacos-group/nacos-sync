package com.alibaba.nacossync.utils;

import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.pojo.request.ClusterAddRequest;
import com.alibaba.nacossync.pojo.request.TaskAddRequest;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class SkyWalkerUtilTest {
    
    /**
     * Test the MD5 hashing function with a sample string.
     */
    @Test
    public void testStringToMd5() {
        String value = "testValue";
        String expectedMd5 = SkyWalkerUtil.stringToMd5(value);
        assertFalse(expectedMd5.isEmpty());
        assertEquals(32, expectedMd5.length());
    }
    
    /**
     * Test the task ID generation using a TaskAddRequest object.
     */
    @Test
    public void testGenerateTaskId_withTaskAddRequest() {
        TaskAddRequest request = new TaskAddRequest();
        request.setServiceName("testService");
        request.setGroupName("testGroup");
        request.setSourceClusterId("sourceCluster");
        request.setDestClusterId("destCluster");
        
        String taskId = SkyWalkerUtil.generateTaskId(request);
        String expectedMd5 = SkyWalkerUtil.stringToMd5("testService_testGroup_sourceCluster_destCluster");
        assertEquals(expectedMd5, taskId);
    }
    
    /**
     * Test the task ID generation using individual parameters.
     */
    @Test
    public void testGenerateTaskId_withParameters() {
        String serviceName = "testService";
        String groupName = "testGroup";
        String sourceClusterId = "sourceCluster";
        String destClusterId = "destCluster";
        
        String taskId = SkyWalkerUtil.generateTaskId(serviceName, groupName, sourceClusterId, destClusterId);
        String expectedMd5 = SkyWalkerUtil.stringToMd5("testService_testGroup_sourceCluster_destCluster");
        assertEquals(expectedMd5, taskId);
    }
    
    /**
     * Test the cluster ID generation using a ClusterAddRequest object.
     */
    @Test
    public void testGenerateClusterId() {
        ClusterAddRequest request = new ClusterAddRequest();
        request.setClusterName("testCluster");
        request.setClusterType("Nacos");
        
        String clusterId = SkyWalkerUtil.generateClusterId(request);
        String expectedMd5 = SkyWalkerUtil.stringToMd5("testCluster_Nacos");
        assertEquals(expectedMd5, clusterId);
    }
    
    /**
     * Test the retrieval of the local IP address.
     * This ensures that the local IP address is not null and is a valid IP address.
     */
    @Test
    public void testGetLocalIp() throws Exception {
        String localIp = SkyWalkerUtil.getLocalIp();
        assertNotNull(localIp);
        assertFalse(localIp.isEmpty());
        try {
            InetAddress ip = InetAddress.getByName(localIp);
            assertNotNull(ip);
        } catch (UnknownHostException e) {
            fail("The IP address is invalid: " + e.getMessage());
        }
    }
    
    /**
     * Test the synchronization key generation using source and destination cluster types.
     */
    @Test
    public void testGenerateSyncKey() {
        ClusterTypeEnum sourceClusterType = ClusterTypeEnum.NACOS;
        ClusterTypeEnum destClusterType = ClusterTypeEnum.ZK;
        
        String syncKey = SkyWalkerUtil.generateSyncKey(sourceClusterType, destClusterType);
        String expectedSyncKey = sourceClusterType.getCode() + ":" + destClusterType.getCode();
        assertEquals(expectedSyncKey, syncKey);
    }
    
    /**
     * Test the operation ID generation to ensure it is unique each time.
     */
    @Test
    public void testGenerateOperationId() {
        String operationId1 = SkyWalkerUtil.generateOperationId();
        String operationId2 = SkyWalkerUtil.generateOperationId();
        assertNotNull(operationId1);
        assertNotNull(operationId2);
        assertNotEquals(operationId1, operationId2);
    }
}

