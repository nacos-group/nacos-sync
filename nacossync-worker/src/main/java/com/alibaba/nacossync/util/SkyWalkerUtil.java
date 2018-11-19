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

import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.UUID;

import com.alibaba.nacossync.constant.SkyWalkerConstants;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.ClusterAddRequest;
import com.alibaba.nacossync.pojo.request.TaskAddRequest;

/**
* @author NacosSync
* @version $Id: SkyWalkerUtil.java, v 0.1 2018-09-26 上午12:10 NacosSync Exp $$
*/
public class SkyWalkerUtil {

    /**
     * 获取字符串md5
     * @param value
     * @return
     */
    public static String StringToMd5(String value) {
        {
            try {
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                md5.update(value.getBytes("UTF-8"));
                byte[] encryption = md5.digest();
                StringBuffer strBuf = new StringBuffer();
                for (int i = 0; i < encryption.length; i++) {
                    if (Integer.toHexString(0xff & encryption[i]).length() == 1) {
                        strBuf.append("0").append(Integer.toHexString(0xff & encryption[i]));
                    } else {
                        strBuf.append(Integer.toHexString(0xff & encryption[i]));
                    }
                }
                return strBuf.toString();
            } catch (NoSuchAlgorithmException e) {
                return "";
            } catch (UnsupportedEncodingException e) {
                return "";
            }
        }
    }

    /**
     * 生成taskId的规则
     * @param addTaskRequest
     * @return
     */
    public static String generateTaskId(TaskAddRequest addTaskRequest) {

        return generateTaskId(addTaskRequest.getServiceName(), addTaskRequest.getGroupName(),
            addTaskRequest.getSourceClusterId(), addTaskRequest.getDestClusterId());
    }

    /**
     * 生成taskId的规则
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
     * 避免获取到回传地址
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

    public static String generateWorkerId(String workerIp) {
        return UUID.randomUUID() + SkyWalkerConstants.UNDERLINE + workerIp;

    }

    public static String getOperationId(TaskDO taskDO) {

        return taskDO.getOperationId();
    }

    public static String generateOperationId() {

        return UUID.randomUUID().toString();
    }
}
