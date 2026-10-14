package com.alibaba.nacossync.template.processor;

import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.event.DeleteTaskEvent;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDeleteRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.google.common.eventbus.EventBus;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskDeleteProcessorTest {

    private final TaskAccessService taskAccessService = mock(TaskAccessService.class);

    private final EventBus eventBus = mock(EventBus.class);

    private final TaskDeleteProcessor processor = new TaskDeleteProcessor(taskAccessService, eventBus);

    @Test
    void unknownTaskIsRejectedLikeDetailAndUpdate() {
        // An id that is not in the database, e.g. a task already deleted from another console tab.
        TaskDeleteRequest request = new TaskDeleteRequest();
        request.setTaskId("missing-task");

        assertThrows(SkyWalkerException.class, () -> processor.process(request, new BaseResult()));

        verify(eventBus, never()).post(any());
        verify(taskAccessService, never()).deleteTaskById(any());
    }

    @Test
    void rowIsRemovedBeforeTheTaskIsStopped() {
        // The sync timer starts every task it finds in the database: once the delete has stopped the task, its row
        // must already be gone.
        TaskDO taskDO = new TaskDO();
        taskDO.setTaskId("task-id");
        taskDO.setServiceName("spring-cloud-eureka-provider");
        when(taskAccessService.findByTaskId("task-id")).thenReturn(taskDO);
        TaskDeleteRequest request = new TaskDeleteRequest();
        request.setTaskId("task-id");

        processor.process(request, new BaseResult());

        InOrder order = inOrder(taskAccessService, eventBus);
        order.verify(taskAccessService).deleteTaskById("task-id");
        order.verify(eventBus).post(any(DeleteTaskEvent.class));
    }
}
