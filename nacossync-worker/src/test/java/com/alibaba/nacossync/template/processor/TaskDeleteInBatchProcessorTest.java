package com.alibaba.nacossync.template.processor;

import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.event.DeleteAllSubTaskEvent;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDeleteInBatchRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.google.common.eventbus.EventBus;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

class TaskDeleteInBatchProcessorTest {

    private final TaskAccessService taskAccessService = mock(TaskAccessService.class);

    private final EventBus eventBus = mock(EventBus.class);

    private final TaskDeleteInBatchProcessor processor = new TaskDeleteInBatchProcessor(taskAccessService, eventBus);

    @Test
    void batchDeleteStopsEachTaskBeforeDeletingTheRows() throws Exception {
        // Like a single delete: without the events the syncs keep running and their instances stay synced.
        TaskDO single = task("single-task", "spring-cloud-eureka-provider");
        TaskDO all = task("all-task", "ALL");
        when(taskAccessService.findByTaskId("single-task")).thenReturn(single);
        when(taskAccessService.findByTaskId("all-task")).thenReturn(all);
        TaskDeleteInBatchRequest request = new TaskDeleteInBatchRequest();
        request.setTaskIds(List.of("single-task", "all-task", "missing-task"));

        processor.process(request, new BaseResult());

        ArgumentCaptor<Object> events = ArgumentCaptor.forClass(Object.class);
        InOrder order = inOrder(eventBus, taskAccessService);
        order.verify(eventBus, times(2)).post(events.capture());
        order.verify(taskAccessService).deleteTaskInBatch(request.getTaskIds());
        assertEquals(DeleteTaskEvent.class, events.getAllValues().get(0).getClass());
        assertSame(single, ((DeleteTaskEvent) events.getAllValues().get(0)).getTaskDO());
        assertEquals(DeleteAllSubTaskEvent.class, events.getAllValues().get(1).getClass());
        assertSame(all, ((DeleteAllSubTaskEvent) events.getAllValues().get(1)).getTaskDO());
    }

    private static TaskDO task(String taskId, String serviceName) {
        TaskDO taskDO = new TaskDO();
        taskDO.setTaskId(taskId);
        taskDO.setServiceName(serviceName);
        return taskDO;
    }
}
