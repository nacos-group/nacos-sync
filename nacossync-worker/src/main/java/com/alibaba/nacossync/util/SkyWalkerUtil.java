/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacossync.util;

import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.ClusterAddRequest;
import com.alibaba.nacossync.pojo.request.TaskAddRequest;
import com.google.common.base.Joiner;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.UUID;

/**
* @author NacosSync
* @version $Id: SkyWalkerUtil.java, v 0.1 2018-09-26 AM12:10 NacosSync Exp $$
*/
public class SkyWalkerUtil {
    
    private static final String SEPARATOR = ":";
    
    /**
     *
     * Gets the string md5
     * @param value The string to be encrypted
     * @return The encrypted string
     */
    public static String StringToMd5(String value) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(value.getBytes(StandardCharsets.UTF_8));
            byte[] encryption = md5.digest();
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
     * The rules of generating taskId
     * @param addTaskRequest
     * @return
     */
    public static String generateTaskId(TaskAddRequest addTaskRequest) {

        return generateTaskId(addTaskRequest.getServiceName(), addTaskRequest.getGroupName(),
                addTaskRequest.getSourceClusterId(), addTaskRequest.getDestClusterId());
    }

    /**
     * The rules of generating taskId
     *
     * @return
     */
    public static String generateTaskId(String serviceName, String groupName,
                                        String sourceClusterId, String destClusterId) {

        StringBuilder sb = new StringBuilder();

        sb.append(serviceName);
        sb.append(SkyWalkerConstants.UNDERLINE);
        sb.append(groupName);
        sb.append(SkyWalkerConstants.UNDERLINE);
        sb.append(sourceClusterId);
        sb.append(SkyWalkerConstants.UNDERLINE);
        sb.append(destClusterId);
        return SkyWalkerUtil.StringToMd5(sb.toString());
    }

    /**
     * 生成集群clusterId的规则
     *
     * @param addClusterRequest
     * @return
     */
    public static String generateClusterId(ClusterAddRequest addClusterRequest) {

        StringBuilder sb = new StringBuilder();
        sb.append(addClusterRequest.getClusterName());
        sb.append(SkyWalkerConstants.UNDERLINE);
        sb.append(addClusterRequest.getClusterType());

        return SkyWalkerUtil.StringToMd5(sb.toString());
    }

    /**
     * Avoid getting a return address
     * @return
     * @throws Exception
     */
    public static String getLocalIp() throws Exception {

        InetAddress addr = InetAddress.getLocalHost();
        String localIp = addr.getHostAddress();
        if (addr.isLoopbackAddress()) {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface in = interfaces.nextElement();
                Enumeration<InetAddress> addrs = in.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    InetAddress address = addrs.nextElement();
                    if (!address.isLoopbackAddress() && address instanceof Inet4Address) {
                        localIp = address.getHostAddress();
                    }
                }
            }
        }
        return localIp;
    }

    public static String generateSyncKey(ClusterTypeEnum sourceClusterType, ClusterTypeEnum destClusterType) {

        return Joiner.on(SEPARATOR).join(sourceClusterType.getCode(), destClusterType.getCode());
    }

    public static String getOperationId(TaskDO taskDO) {

        return taskDO.getOperationId();
    }

    public static String generateOperationId() {

        return UUID.randomUUID().toString();
    }
    
    
    
    
}
