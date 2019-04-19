package com.alibaba.nacossync.extension.client.impl;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacossync.extension.client.InstanceQueryModel;
import com.alibaba.nacossync.extension.client.SyncQueryClient;
import com.alibaba.nacossync.extension.holder.NacosServerHolder;
import com.alibaba.nacossync.pojo.view.TaskModel;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class NacosSyncQueryClientImpl implements SyncQueryClient {

    private final NacosServerHolder nacosServerHolder;

    public NacosSyncQueryClientImpl(
            NacosServerHolder nacosServerHolder) {
        this.nacosServerHolder = nacosServerHolder;
    }

    @Override
    public List<TaskModel> getAllInstance(InstanceQueryModel instanceQueryModel) {
        NamingService namingService = nacosServerHolder
                .get(instanceQueryModel.getSourceClusterId(), instanceQueryModel.getGroupName());
        try {
            ListView<String> servicesOfServer = namingService
                    .getServicesOfServer(instanceQueryModel.getPageNo(),
                            instanceQueryModel.getPageSize());
            return servicesOfServer.getData().stream()
                    .map(serviceName -> buildTaskModel(instanceQueryModel, serviceName))
                    .collect(Collectors.toList());

        } catch (NacosException e) {
            log.error("When using nacos client failure query tasks", e);
            return Collections.emptyList();
        }
    }

    private TaskModel buildTaskModel(InstanceQueryModel instanceQueryModel, String serviceName) {
        TaskModel taskModel = new TaskModel();
        taskModel.setServiceName(serviceName);
        taskModel.setSourceClusterId(instanceQueryModel.getSourceClusterId());
        taskModel.setDestClusterId(instanceQueryModel.getDestClusterId());
        return taskModel;
    }


}
