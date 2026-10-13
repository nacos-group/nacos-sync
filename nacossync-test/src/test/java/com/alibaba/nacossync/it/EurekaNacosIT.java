package com.alibaba.nacossync.it;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.HttpClientErrorException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Eureka (embedded server) <-> Nacos A. Eureka sources are polled every 3 s; Nacos sources push via subscribe.
 */
class EurekaNacosIT extends AbstractSyncIT {

    private static final int PORT = 8080;

    @Test
    void eurekaInstanceIsSyncedToNacosAndRemovedOnDeregister() throws Exception {
        String app = uniqueName("E2N");
        String ip = randomIp();
        String instanceId = registerEureka(app, ip, Map.of("zone", "it-zone"));
        addTask(cluster.eureka(), cluster.nacosA(), app, DEFAULT_GROUP);

        eventually().untilAsserted(() -> {
            Map<String, String> metadata = requireNacos(infra.namingA(), app, DEFAULT_GROUP, ip, PORT).getMetadata();
            assertThat(metadata).containsEntry("zone", "it-zone");
            assertSyncMarkers(metadata, cluster.eureka(), cluster.nacosA(), "EUREKA");
        });

        infra.eureka().deregister(app, instanceId);
        eventually().untilAsserted(() -> assertThat(nacosInstances(infra.namingA(), app, DEFAULT_GROUP)).isEmpty());
    }

    @Test
    void outOfServiceEurekaInstanceLeavesNacosAndUpBringsItBack() throws Exception {
        String app = uniqueName("E2N-OOS");
        String ip = randomIp();
        String instanceId = registerEureka(app, ip, Map.of());
        addTask(cluster.eureka(), cluster.nacosA(), app, DEFAULT_GROUP);
        eventually().untilAsserted(() -> requireNacos(infra.namingA(), app, DEFAULT_GROUP, ip, PORT));

        infra.eureka().setStatus(app, instanceId, "OUT_OF_SERVICE");
        eventually().untilAsserted(
                () -> assertThat(findNacos(infra.namingA(), app, DEFAULT_GROUP, ip, PORT)).isEmpty());

        infra.eureka().setStatus(app, instanceId, "UP");
        eventually().untilAsserted(() -> requireNacos(infra.namingA(), app, DEFAULT_GROUP, ip, PORT));
    }

    @Test
    void eurekaMetadataUpdateReachesNacos() throws Exception {
        String app = uniqueName("E2N-META");
        String ip = randomIp();
        String instanceId = registerEureka(app, ip, Map.of("version", "v1"));
        addTask(cluster.eureka(), cluster.nacosA(), app, DEFAULT_GROUP);
        eventually().untilAsserted(() -> assertThat(
                requireNacos(infra.namingA(), app, DEFAULT_GROUP, ip, PORT).getMetadata()).containsEntry("version",
                "v1"));

        infra.eureka().putMetadata(app, instanceId, "version", "v2");
        eventually().untilAsserted(() -> assertThat(
                requireNacos(infra.namingA(), app, DEFAULT_GROUP, ip, PORT).getMetadata()).containsEntry("version",
                "v2"));
    }

    @Test
    void nacosInstanceIsSyncedToEurekaAndRemovedOnDeregister() throws Exception {
        String service = uniqueName("N2E");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of("zone", "it-zone"));
        addTask(cluster.nacosA(), cluster.eureka(), service, DEFAULT_GROUP);

        eventually().untilAsserted(() -> {
            JsonNode synced = requireEureka(service, ip);
            assertThat(synced.path("status").asText()).isEqualTo("UP");
            Map<String, String> metadata = metadataOf(synced);
            assertThat(metadata).containsEntry("zone", "it-zone");
            assertSyncMarkers(metadata, cluster.nacosA(), cluster.eureka(), "NACOS");
        });

        infra.namingA().deregisterInstance(service, DEFAULT_GROUP, ip, PORT);
        eventually().untilAsserted(() -> assertThat(infra.eureka().instances(service)).isEmpty());
    }

    @Test
    void disabledNacosInstanceIsRemovedFromEureka() throws Exception {
        String service = uniqueName("N2E-OFF");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of());
        addTask(cluster.nacosA(), cluster.eureka(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> requireEureka(service, ip));

        Instance disabled = nacosInstance(ip, PORT, Map.of());
        disabled.setEnabled(false);
        infra.namingA().registerInstance(service, DEFAULT_GROUP, disabled);

        eventually().untilAsserted(() -> assertThat(infra.eureka().find(service, ip, PORT)).isEmpty());
    }

    @Test
    void nacosMetadataUpdateReachesEureka() throws Exception {
        String service = uniqueName("N2E-META");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of("version", "v1"));
        addTask(cluster.nacosA(), cluster.eureka(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> assertThat(metadataOf(requireEureka(service, ip))).containsEntry("version",
                "v1"));

        // Re-registering the same ip:port from the same client replaces its metadata.
        infra.namingA().registerInstance(service, DEFAULT_GROUP, nacosInstance(ip, PORT, Map.of("version", "v2")));

        eventually().untilAsserted(() -> assertThat(metadataOf(requireEureka(service, ip))).containsEntry("version",
                "v2"));
    }

    @Test
    void bidirectionalTasksDoNotEchoInstancesBack() throws Exception {
        String service = uniqueName("E-BI");
        String eurekaIp = randomIp();
        String nacosIp = randomIp();
        registerEureka(service, eurekaIp, Map.of());
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, nacosIp, PORT, Map.of());
        addTask(cluster.eureka(), cluster.nacosA(), service, DEFAULT_GROUP);
        addTask(cluster.nacosA(), cluster.eureka(), service, DEFAULT_GROUP);

        eventually().untilAsserted(() -> assertOneNativeAndOneCopyEachSide(service, eurekaIp, nacosIp));
        stable().untilAsserted(() -> assertOneNativeAndOneCopyEachSide(service, eurekaIp, nacosIp));
    }

    private void assertOneNativeAndOneCopyEachSide(String service, String eurekaIp, String nacosIp) throws Exception {
        List<Instance> inNacos = nacosInstances(infra.namingA(), service, DEFAULT_GROUP);
        assertThat(inNacos).extracting(Instance::getIp).containsExactlyInAnyOrder(nacosIp, eurekaIp);
        for (Instance instance : inNacos) {
            if (instance.getIp().equals(nacosIp)) {
                assertNoSyncMarkers(instance.getMetadata());
            } else {
                assertSyncMarkers(instance.getMetadata(), cluster.eureka(), cluster.nacosA(), "EUREKA");
            }
        }
        List<JsonNode> inEureka = infra.eureka().instances(service);
        assertThat(inEureka).extracting(i -> i.path("ipAddr").asText())
                .containsExactlyInAnyOrder(eurekaIp, nacosIp);
        for (JsonNode instance : inEureka) {
            if (instance.path("ipAddr").asText().equals(eurekaIp)) {
                assertNoSyncMarkers(metadataOf(instance));
            } else {
                assertSyncMarkers(metadataOf(instance), cluster.nacosA(), cluster.eureka(), "NACOS");
            }
        }
    }

    private String registerEureka(String app, String ip, Map<String, String> metadata) {
        String instanceId = infra.eureka().register(app, ip, PORT, metadata);
        afterTest(() -> {
            try {
                infra.eureka().deregister(app, instanceId);
            } catch (HttpClientErrorException.NotFound alreadyGone) {
                // the test deregistered it itself
            }
        });
        return instanceId;
    }

    private JsonNode requireEureka(String app, String ip) {
        return infra.eureka().find(app, ip, PORT).orElseThrow(
                () -> new AssertionError(ip + ":" + PORT + " not in Eureka app " + app + ": "
                        + infra.eureka().instances(app)));
    }

    private static Map<String, String> metadataOf(JsonNode eurekaInstance) {
        Map<String, String> metadata = new HashMap<>();
        eurekaInstance.path("metadata").fields().forEachRemaining(e -> metadata.put(e.getKey(), e.getValue().asText()));
        return metadata;
    }
}
