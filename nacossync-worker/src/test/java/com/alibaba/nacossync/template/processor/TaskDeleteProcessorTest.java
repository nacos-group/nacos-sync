package com.alibaba.nacossync.template.processor;

import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.extension.TaskDispatcher;
import com.alibaba.nacossync.pojo.model.TaskDO;
import com.alibaba.nacossync.pojo.request.TaskDeleteRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.alibaba.nacossync.timer.CheckRunningStatusAllNacosThread;
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

    private final TaskDispatcher taskDispatcher = mock(TaskDispatcher.class);

    private final CheckRunningStatusAllNacosThread checkRunningStatusAllNacosThread =
            mock(CheckRunningStatusAllNacosThread.class);

    private final TaskDeleteProcessor processor = new TaskDeleteProcessor(taskAccessService, taskDispatcher,
            checkRunningStatusAllNacosThread);

    @Test
    void unknownTaskIsRejectedLikeDetailAndUpdate() {
        // An id that is not in the database, e.g. a task already deleted from another console tab.
        TaskDeleteRequest request = new TaskDeleteRequest();
        request.setTaskId("missing-task");

        assertThrows(SkyWalkerException.class, () -> processor.process(request, new BaseResult()));

        verify(taskDispatcher, never()).delete(any());
        verify(checkRunningStatusAllNacosThread, never()).deleteAllSubTasks(any());
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

        InOrder order = inOrder(taskAccessService, taskDispatcher);
        order.verify(taskAccessService).deleteTaskById("task-id");
        order.verify(taskDispatcher).delete(taskDO);
    }
}
