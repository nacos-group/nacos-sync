package com.alibaba.nacossync.extension.sharding;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.util.SkyWalkerUtil;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by maj on 2020/10/27.
 */
@Service
@Lazy
public class ConsistentHashServiceSharding extends AbstractServiceSharding {

    public static final String HASH_NODES = "-hash.vn.nodes";

    private List<String> nodes = new LinkedList<String>();

    private SortedMap<Integer, String> virtualNodes = new TreeMap<Integer, String>();

    private static final int VIRTUAL_COUNT = 100;

    public ConsistentHashServiceSharding() {
        super();
    }


    @Override
    public void doSharding() {
        List<String> servers = getServers();
        nodes.clear();
        virtualNodes.clear();
        for (String node : servers) {
            nodes.add(node);
        }
        for (String node : nodes) {
            for (int i = 0; i < VIRTUAL_COUNT; i++) {
                String virtualNodeName = node + HASH_NODES + String.valueOf(i);
                virtualNodes.put(getHash(virtualNodeName), virtualNodeName);
            }
        }
    }

    @Override
    public String getShardingServer(String key) {
        int hash = getHash(SkyWalkerUtil.StringToMd5(key));
        SortedMap<Integer, String> subMap = virtualNodes.tailMap(hash);
        String virtualNode;
        if (subMap.isEmpty()) {
            if(virtualNodes.isEmpty()){
                return "";
            }
            Integer i = virtualNodes.firstKey();
            virtualNode = virtualNodes.get(i);
        } else {
            Integer i = subMap.firstKey();
            virtualNode = subMap.get(i);
        }
        if (StringUtils.isNotBlank(virtualNode)) {
            return virtualNode.substring(0, virtualNode.indexOf("-"));
        }
        return null;
    }

    private int getHash(String str) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < str.length(); i++)
            hash = (hash ^ str.charAt(i)) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        if (hash < 0)
            hash = Math.abs(hash);
        return hash;
    }

    @Override
    public boolean isProcess(TaskDO taskDO, String serviceName) {
        return false;
    }
}
