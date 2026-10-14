package com.alibaba.nacossync.template.processor;

import com.alibaba.nacossync.dao.TaskAccessService;
import com.alibaba.nacossync.exception.SkyWalkerException;
import com.alibaba.nacossync.pojo.request.TaskDeleteRequest;
import com.alibaba.nacossync.pojo.result.BaseResult;
import com.google.common.eventbus.EventBus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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
}
