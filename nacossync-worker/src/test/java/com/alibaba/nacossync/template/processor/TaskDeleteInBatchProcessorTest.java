package com.alibaba.nacossync.template.processor;

import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.extension.TaskDispatcher;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDeleteInBatchRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.timer.CheckRunningStatusAllNacosThread;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskDeleteInBatchProcessorTest {

    private final TaskAccessService taskAccessService = mock(TaskAccessService.class);

    private final TaskDispatcher taskDispatcher = mock(TaskDispatcher.class);

    private final CheckRunningStatusAllNacosThread checkRunningStatusAllNacosThread =
            mock(CheckRunningStatusAllNacosThread.class);

    private final TaskDeleteInBatchProcessor processor = new TaskDeleteInBatchProcessor(taskAccessService,
            taskDispatcher, checkRunningStatusAllNacosThread);

    @Test
    void batchDeleteRemovesEachRowAndThenStopsItsTask() throws Exception {
        // Like a single delete: without stopping the tasks the syncs keep running and their instances stay synced.
        // The row goes first, or the sync timer starts the task again from the row once the delete has stopped it.
        TaskDO single = task("single-task", "spring-cloud-eureka-provider");
        TaskDO all = task("all-task", "ALL");
        when(taskAccessService.findByTaskId("single-task")).thenReturn(single);
        when(taskAccessService.findByTaskId("all-task")).thenReturn(all);
        TaskDeleteInBatchRequest request = new TaskDeleteInBatchRequest();
        request.setTaskIds(List.of("single-task", "all-task", "missing-task"));

        processor.process(request, new BaseResult());

        InOrder order = inOrder(taskAccessService, taskDispatcher, checkRunningStatusAllNacosThread);
        order.verify(taskAccessService).deleteTaskById("single-task");
        order.verify(taskDispatcher).delete(single);
        order.verify(taskAccessService).deleteTaskById("all-task");
        order.verify(checkRunningStatusAllNacosThread).deleteAllSubTasks(all);
        verify(taskAccessService, never()).deleteTaskById("missing-task");
        verify(taskDispatcher, never()).delete(all);
        verify(checkRunningStatusAllNacosThread, never()).deleteAllSubTasks(single);
    }

    private static TaskDO task(String taskId, String serviceName) {
        TaskDO taskDO = new TaskDO();
        taskDO.setTaskId(taskId);
        taskDO.setServiceName(serviceName);
        return taskDO;
    }
}
