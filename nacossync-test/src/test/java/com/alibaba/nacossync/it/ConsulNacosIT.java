package com.alibaba.nacossync.it;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.model.HealthService;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Consul (dev agent) <-> Nacos A. Consul sources are polled every 3 s; metadata travels as key=value tags.
 */
class ConsulNacosIT extends AbstractSyncIT {

    private static final int PORT = 9090;

    @Test
    void consulServiceIsSyncedToNacosAndRemovedOnDeregister() throws Exception {
        String service = uniqueName("C2N");
        String ip = randomIp();
        String consulId = registerConsul(service, ip, Map.of("zone", "it-zone"));
        addTask(cluster.consul(), cluster.nacosA(), service, DEFAULT_GROUP);

        eventually().untilAsserted(() -> {
            Map<String, String> metadata = requireNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT)
                    .getMetadata();
            assertThat(metadata).containsEntry("zone", "it-zone");
            assertSyncMarkers(metadata, cluster.consul(), cluster.nacosA(), "CONSUL");
        });

        infra.consul().agentServiceDeregister(consulId);
        eventually().untilAsserted(
                () -> assertThat(nacosInstances(infra.namingA(), service, DEFAULT_GROUP)).isEmpty());
    }

    @Test
    void nacosInstanceIsSyncedToConsulAndRemovedOnDeregister() throws Exception {
        String service = uniqueName("N2C");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of("zone", "it-zone"));
        addTask(cluster.nacosA(), cluster.consul(), service, DEFAULT_GROUP);

        eventually().untilAsserted(() -> {
            List<HealthService.Service> inConsul = consulServices(service);
            assertThat(inConsul).singleElement().satisfies(s -> {
                assertThat(s.getAddress()).isEqualTo(ip);
                assertThat(s.getPort()).isEqualTo(PORT);
                assertThat(tagsOf(s)).containsEntry("zone", "it-zone");
                assertSyncMarkers(tagsOf(s), cluster.nacosA(), cluster.consul(), "NACOS");
            });
        });

        infra.namingA().deregisterInstance(service, DEFAULT_GROUP, ip, PORT);
        eventually().untilAsserted(() -> assertThat(consulServices(service)).isEmpty());
    }

    @Test
    void nacosToConsulKeepsTheRemainingPeerWhenOneInstanceLeaves() throws Exception {
        String service = uniqueName("N2C-PEER");
        String stayingIp = randomIp();
        String leavingIp = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, stayingIp, PORT, Map.of());
        NamingService secondApp = newNacosClient(infra.nacosA());
        registerNacos(secondApp, service, DEFAULT_GROUP, leavingIp, PORT, Map.of());
        addTask(cluster.nacosA(), cluster.consul(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> assertThat(consulAddresses(service))
                .containsExactlyInAnyOrder(stayingIp, leavingIp));

        secondApp.deregisterInstance(service, DEFAULT_GROUP, leavingIp, PORT);

        eventually().untilAsserted(() -> assertThat(consulAddresses(service)).containsExactly(stayingIp));
        stable().untilAsserted(() -> assertThat(consulAddresses(service)).containsExactly(stayingIp));
    }

    @Test
    void bidirectionalTasksDoNotEchoInstancesBack() throws Exception {
        String service = uniqueName("C-BI");
        String consulIp = randomIp();
        String nacosIp = randomIp();
        registerConsul(service, consulIp, Map.of());
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, nacosIp, PORT, Map.of());
        addTask(cluster.consul(), cluster.nacosA(), service, DEFAULT_GROUP);
        addTask(cluster.nacosA(), cluster.consul(), service, DEFAULT_GROUP);

        eventually().untilAsserted(() -> assertOneNativeAndOneCopyEachSide(service, consulIp, nacosIp));
        stable().untilAsserted(() -> assertOneNativeAndOneCopyEachSide(service, consulIp, nacosIp));
    }

    private void assertOneNativeAndOneCopyEachSide(String service, String consulIp, String nacosIp)
            throws Exception {
        List<Instance> inNacos = nacosInstances(infra.namingA(), service, DEFAULT_GROUP);
        assertThat(inNacos).extracting(Instance::getIp).containsExactlyInAnyOrder(nacosIp, consulIp);
        for (Instance instance : inNacos) {
            if (instance.getIp().equals(nacosIp)) {
                assertNoSyncMarkers(instance.getMetadata());
            } else {
                assertSyncMarkers(instance.getMetadata(), cluster.consul(), cluster.nacosA(), "CONSUL");
            }
        }
        List<HealthService.Service> inConsul = consulServices(service);
        assertThat(inConsul).extracting(HealthService.Service::getAddress)
                .containsExactlyInAnyOrder(consulIp, nacosIp);
        for (HealthService.Service consulService : inConsul) {
            if (consulService.getAddress().equals(consulIp)) {
                assertNoSyncMarkers(tagsOf(consulService));
            } else {
                assertSyncMarkers(tagsOf(consulService), cluster.nacosA(), cluster.consul(), "NACOS");
            }
        }
    }

    private String registerConsul(String service, String ip, Map<String, String> tags) {
        String id = service + "-" + ip + "-" + PORT;
        NewService newService = new NewService();
        newService.setId(id);
        newService.setName(service);
        newService.setAddress(ip);
        newService.setPort(PORT);
        newService.setTags(tags.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toList());
        infra.consul().agentServiceRegister(newService);
        afterTest(() -> infra.consul().agentServiceDeregister(id));
        return id;
    }

    /** All registered instances regardless of health. */
    private List<HealthService.Service> consulServices(String service) {
        return infra.consul().getHealthServices(service, false, QueryParams.DEFAULT).getValue().stream()
                .map(HealthService::getService).toList();
    }

    private List<String> consulAddresses(String service) {
        return consulServices(service).stream().map(HealthService.Service::getAddress).toList();
    }

    private static Map<String, String> tagsOf(HealthService.Service service) {
        Map<String, String> tags = new HashMap<>();
        for (String tag : service.getTags()) {
            String[] keyValue = tag.split("=", 2);
            if (keyValue.length == 2) {
                tags.put(keyValue[0], keyValue[1]);
            }
        }
        return tags;
    }
}
