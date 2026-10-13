package com.alibaba.nacossync.it;

import com.alibaba.nacos.api.naming.pojo.Instance;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task add / pause / resume / delete on a Nacos A -> Nacos B task.
 */
class TaskLifecycleIT extends AbstractSyncIT {

    private static final int PORT = 8080;

    @Test
    void deletingTaskRemovesSyncedInstancesButKeepsNativeOnes() throws Exception {
        String service = uniqueName("TASK-DEL");
        String sourceIp = randomIp();
        String nativeIp = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, sourceIp, PORT, Map.of());
        registerNacos(infra.namingB(), service, DEFAULT_GROUP, nativeIp, PORT, Map.of());
        String taskId = addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> assertThat(ipsInB(service)).containsExactlyInAnyOrder(sourceIp, nativeIp));

        api.deleteTask(taskId);

        eventually().untilAsserted(() -> assertThat(ipsInB(service)).containsExactly(nativeIp));
    }

    @Test
    void pausingTaskRemovesSyncedInstancesAndResumingRestoresThem() throws Exception {
        String service = uniqueName("TASK-PAUSE");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of());
        String taskId = addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> assertThat(ipsInB(service)).containsExactly(ip));

        api.updateTaskStatus(taskId, STATUS_PAUSED);
        eventually().untilAsserted(() -> assertThat(ipsInB(service)).isEmpty());
        assertThat(api.taskIds()).as("a paused task keeps its row").contains(taskId);

        api.updateTaskStatus(taskId, STATUS_SYNC);
        eventually().untilAsserted(() -> assertThat(ipsInB(service)).containsExactly(ip));
    }

    @Test
    void reAddingDeletedTaskSyncsAgain() throws Exception {
        String service = uniqueName("TASK-READD");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of());
        String taskId = addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> assertThat(ipsInB(service)).containsExactly(ip));
        api.deleteTask(taskId);
        eventually().untilAsserted(() -> assertThat(ipsInB(service)).isEmpty());

        assertThat(addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP)).isEqualTo(taskId);

        eventually().untilAsserted(() -> assertThat(ipsInB(service)).containsExactly(ip));
    }

    @Test
    void deletingTasksInBatchRemovesSyncedInstances() throws Exception {
        String service = uniqueName("TASK-BATCH");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of());
        String taskId = addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> assertThat(ipsInB(service)).containsExactly(ip));

        api.deleteTasksInBatch(List.of(taskId));
        assertThat(api.taskIds()).doesNotContain(taskId);

        eventually().untilAsserted(() -> assertThat(ipsInB(service)).isEmpty());
    }

    private List<String> ipsInB(String service) throws Exception {
        return nacosInstances(infra.namingB(), service, DEFAULT_GROUP).stream().map(Instance::getIp).toList();
    }
}
