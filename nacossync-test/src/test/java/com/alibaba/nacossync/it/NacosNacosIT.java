package com.alibaba.nacossync.it;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Nacos A <-> Nacos B. Nacos sources push changes via subscribe.
 */
class NacosNacosIT extends AbstractSyncIT {

    private static final int PORT = 8080;

    private static final String ALL_SERVICES = "ALL";

    @Test
    void instanceIsSyncedToOtherNacosAndRemovedOnDeregister() throws Exception {
        String service = uniqueName("N2N");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of("zone", "it-zone"));
        addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);

        eventually().untilAsserted(() -> {
            Map<String, String> metadata = requireNacos(infra.namingB(), service, DEFAULT_GROUP, ip, PORT)
                    .getMetadata();
            assertThat(metadata).containsEntry("zone", "it-zone");
            assertSyncMarkers(metadata, cluster.nacosA(), cluster.nacosB(), "NACOS");
            assertThat(metadata.get(SYNC_INSTANCE_TAG)).startsWith(cluster.nacosA() + "@@");
        });

        infra.namingA().deregisterInstance(service, DEFAULT_GROUP, ip, PORT);
        eventually().untilAsserted(
                () -> assertThat(nacosInstances(infra.namingB(), service, DEFAULT_GROUP)).isEmpty());
    }

    @Test
    void syncedInstanceStaysRegisteredWhileTaskRuns() throws Exception {
        String service = uniqueName("N2N-STABLE");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of());
        addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> requireNacos(infra.namingB(), service, DEFAULT_GROUP, ip, PORT));

        stable().untilAsserted(() -> requireNacos(infra.namingB(), service, DEFAULT_GROUP, ip, PORT));
    }

    @Test
    void onlyTheDeregisteredInstanceIsRemoved() throws Exception {
        String service = uniqueName("N2N-PEER");
        String stayingIp = randomIp();
        String leavingIp = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, stayingIp, PORT, Map.of());
        NamingService secondApp = newNacosClient(infra.nacosA());
        registerNacos(secondApp, service, DEFAULT_GROUP, leavingIp, PORT, Map.of());
        addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);
        eventually().untilAsserted(
                () -> assertThat(nacosInstances(infra.namingB(), service, DEFAULT_GROUP)).extracting(Instance::getIp)
                        .containsExactlyInAnyOrder(stayingIp, leavingIp));

        secondApp.deregisterInstance(service, DEFAULT_GROUP, leavingIp, PORT);

        eventually().untilAsserted(
                () -> assertThat(nacosInstances(infra.namingB(), service, DEFAULT_GROUP)).extracting(Instance::getIp)
                        .containsExactly(stayingIp));
        // Whether the peer stays put is syncedInstanceStaysRegisteredWhileTaskRuns; here: the leaver stays gone.
        stable().untilAsserted(
                () -> assertThat(nacosInstances(infra.namingB(), service, DEFAULT_GROUP)).extracting(Instance::getIp)
                        .doesNotContain(leavingIp));
    }

    @Test
    void reverseTaskDoesNotEchoInstancesBack() throws Exception {
        String service = uniqueName("N-BI");
        String ipInA = randomIp();
        String ipInB = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ipInA, PORT, Map.of());
        registerNacos(infra.namingB(), service, DEFAULT_GROUP, ipInB, PORT, Map.of());
        addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);
        addTask(cluster.nacosB(), cluster.nacosA(), service, DEFAULT_GROUP);

        eventually().untilAsserted(() -> assertOneNativeAndOneCopyEachSide(service, ipInA, ipInB));
        stable().untilAsserted(() -> assertNothingEchoedBack(service, ipInA, ipInB));
    }

    @Test
    void allTaskCopiesEveryServiceOfTheGroup() throws Exception {
        String group = uniqueName("ALL-GROUP");
        String first = uniqueName("N2N-ALL-1");
        String second = uniqueName("N2N-ALL-2");
        String firstIp = randomIp();
        String secondIp = randomIp();
        registerNacos(infra.namingA(), first, group, firstIp, PORT, Map.of());
        registerNacos(infra.namingA(), second, group, secondIp, PORT, Map.of());
        addTask(cluster.nacosA(), cluster.nacosB(), ALL_SERVICES, group);

        eventually().untilAsserted(() -> {
            assertSyncMarkers(requireNacos(infra.namingB(), first, group, firstIp, PORT).getMetadata(),
                    cluster.nacosA(), cluster.nacosB(), "NACOS");
            assertSyncMarkers(requireNacos(infra.namingB(), second, group, secondIp, PORT).getMetadata(),
                    cluster.nacosA(), cluster.nacosB(), "NACOS");
        });
    }

    @Test
    void deletingAllTaskRemovesSyncedInstances() throws Exception {
        String group = uniqueName("ALL-DEL-GROUP");
        String service = uniqueName("N2N-ALL-DEL");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, group, ip, PORT, Map.of());
        String taskId = addTask(cluster.nacosA(), cluster.nacosB(), ALL_SERVICES, group);
        eventually().untilAsserted(() -> requireNacos(infra.namingB(), service, group, ip, PORT));

        api.deleteTask(taskId);

        eventually().untilAsserted(() -> assertThat(nacosInstances(infra.namingB(), service, group)).isEmpty());
    }

    @Test
    void metadataUpdateReachesOtherNacos() throws Exception {
        String service = uniqueName("N2N-META");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of("version", "v1"));
        addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> assertThat(
                requireNacos(infra.namingB(), service, DEFAULT_GROUP, ip, PORT).getMetadata()).containsEntry("version",
                "v1"));

        // Re-registering the same ip:port from the same client replaces its metadata.
        infra.namingA().registerInstance(service, DEFAULT_GROUP, nacosInstance(ip, PORT, Map.of("version", "v2")));

        eventually().untilAsserted(() -> assertThat(
                requireNacos(infra.namingB(), service, DEFAULT_GROUP, ip, PORT).getMetadata()).containsEntry("version",
                "v2"));
    }

    /**
     * An echo would add a second copy (duplicate ip) or mark a native instance. Whether the copy itself stays
     * registered is covered by {@link #syncedInstanceStaysRegisteredWhileTaskRuns()}, so it may be absent here.
     */
    private void assertNothingEchoedBack(String service, String ipInA, String ipInB) throws Exception {
        assertNativeKeptAndNoEcho(infra.namingA(), service, ipInA, ipInB, cluster.nacosB(), cluster.nacosA());
        assertNativeKeptAndNoEcho(infra.namingB(), service, ipInB, ipInA, cluster.nacosA(), cluster.nacosB());
    }

    private static void assertNativeKeptAndNoEcho(NamingService naming, String service, String nativeIp,
            String copiedIp, String copiedFrom, String copiedTo) throws Exception {
        List<Instance> instances = nacosInstances(naming, service, DEFAULT_GROUP);
        assertThat(instances).extracting(Instance::getIp).contains(nativeIp).doesNotHaveDuplicates()
                .isSubsetOf(nativeIp, copiedIp);
        assertMarkers(instances, nativeIp, copiedFrom, copiedTo);
    }

    private void assertOneNativeAndOneCopyEachSide(String service, String ipInA, String ipInB) throws Exception {
        assertOneNativeAndOneCopy(infra.namingA(), service, ipInA, ipInB, cluster.nacosB(), cluster.nacosA());
        assertOneNativeAndOneCopy(infra.namingB(), service, ipInB, ipInA, cluster.nacosA(), cluster.nacosB());
    }

    private static void assertOneNativeAndOneCopy(NamingService naming, String service, String nativeIp,
            String copiedIp, String copiedFrom, String copiedTo) throws Exception {
        List<Instance> instances = nacosInstances(naming, service, DEFAULT_GROUP);
        assertThat(instances).extracting(Instance::getIp).containsExactlyInAnyOrder(nativeIp, copiedIp);
        assertMarkers(instances, nativeIp, copiedFrom, copiedTo);
    }

    private static void assertMarkers(List<Instance> instances, String nativeIp, String copiedFrom,
            String copiedTo) {
        for (Instance instance : instances) {
            if (instance.getIp().equals(nativeIp)) {
                assertNoSyncMarkers(instance.getMetadata());
            } else {
                assertSyncMarkers(instance.getMetadata(), copiedFrom, copiedTo, "NACOS");
            }
        }
    }
}
