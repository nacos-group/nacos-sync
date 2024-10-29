package com.alibaba.nacossync.util;

import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.ClusterAddRequest;
import com.alibaba.nacossync.pojo.request.TaskAddRequest;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.UUID;

/**
 * Utility class for various operations in SkyWalker.
 */
public class SkyWalkerUtil {
    
    private static final String SEPARATOR = ":";
    private static final String MD5_ALGORITHM = "MD5";
    
    /**
     * Generates an MD5 hash for the given string.
     *
     * @param value The string to be encrypted.
     * @return The encrypted string, or an empty string if encryption fails.
     */
    public static String stringToMd5(String value) {
        if (StringUtils.isBlank(value)) {
            return "";
        }
        try {
            MessageDigest md5 = MessageDigest.getInstance(MD5_ALGORITHM);
            byte[] encryption = md5.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder strBuf = new StringBuilder();
            for (byte b : encryption) {
                strBuf.append(String.format("%02x", b));
            }
            return strBuf.toString();
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }
    
    /**
     * Generates a task ID based on the given TaskAddRequest.
     *
     * @param addTaskRequest The TaskAddRequest containing task details.
     * @return The generated task ID.
     */
    public static String generateTaskId(TaskAddRequest addTaskRequest) {
        return generateTaskId(addTaskRequest.getServiceName(), addTaskRequest.getGroupName(),
                addTaskRequest.getSourceClusterId(), addTaskRequest.getDestClusterId());
    }
    
    /**
     * Generates a task ID based on the given parameters.
     *
     * @param serviceName     The service name.
     * @param groupName       The group name.
     * @param sourceClusterId The source cluster ID.
     * @param destClusterId   The destination cluster ID.
     * @return The generated task ID.
     */
    public static String generateTaskId(String serviceName, String groupName,
            String sourceClusterId, String destClusterId) {
        String rawId = String.join(SkyWalkerConstants.UNDERLINE, serviceName, groupName, sourceClusterId, destClusterId);
        return stringToMd5(rawId);
    }
    
    /**
     * Generates a cluster ID based on the given ClusterAddRequest.
     *
     * @param addClusterRequest The ClusterAddRequest containing cluster details.
     * @return The generated cluster ID.
     */
    public static String generateClusterId(ClusterAddRequest addClusterRequest) {
        String rawId = String.join(SkyWalkerConstants.UNDERLINE, addClusterRequest.getClusterName(), addClusterRequest.getClusterType());
        return stringToMd5(rawId);
    }
    
    /**
     * Gets the local IP address, avoiding loopback addresses.
     *
     * @return The local IP address.
     * @throws Exception If an error occurs while fetching the IP address.
     */
    public static String getLocalIp() throws Exception {
        InetAddress addr = InetAddress.getLocalHost();
        if (!addr.isLoopbackAddress()) {
            return addr.getHostAddress();
        }
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
            while (addrs.hasMoreElements()) {
                InetAddress address = addrs.nextElement();
                if (!address.isLoopbackAddress() && address instanceof Inet4Address) {
                    return address.getHostAddress();
                }
            }
        }
        return addr.getHostAddress();
    }
    
    /**
     * Generates a synchronization key based on source and destination cluster types.
     *
     * @param sourceClusterType The source cluster type.
     * @param destClusterType   The destination cluster type.
     * @return The generated synchronization key.
     */
    public static String generateSyncKey(ClusterTypeEnum sourceClusterType, ClusterTypeEnum destClusterType) {
        return Joiner.on(SEPARATOR).join(sourceClusterType.getCode(), destClusterType.getCode());
    }
    
    /**
     * Gets the operation ID from the given TaskDO.
     *
     * @param taskDO The TaskDO containing the operation ID.
     * @return The operation ID.
     */
    public static String getOperationId(TaskDO taskDO) {
        return taskDO.getOperationId();
    }
    
    /**
     * Generates a unique operation ID.
     *
     * @return The generated operation ID.
     */
    public static String generateOperationId() {
        return UUID.randomUUID().toString();
    }
}


