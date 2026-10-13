package com.alibaba.nacossync.it;

import com.alibaba.nacos.api.naming.pojo.Instance;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.Test;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Zookeeper (Dubbo registry layout) <-> Nacos A. Providers live under {@code /dubbo/<interface>/providers} as
 * URL-encoded Dubbo URLs; Zookeeper sources are watched with a CuratorCache.
 */
class ZookeeperNacosIT extends AbstractSyncIT {

    private static final int DUBBO_PORT = 20880;

    private static final String VERSION = "1.0.0";

    @Test
    void dubboProviderIsSyncedToNacos() throws Exception {
        String dubboInterface = uniqueInterface("ZkToNacos");
        String dubboGroup = uniqueName("ZK-GROUP");
        String ip = randomIp();
        createDubboProvider(dubboInterface, dubboGroup, ip);
        // For a Zookeeper source the task group filters the Dubbo group and is also the Nacos destination group.
        addTask(cluster.zookeeper(), cluster.nacosA(), dubboInterface, dubboGroup, VERSION);

        eventually().untilAsserted(() -> {
            Map<String, String> metadata = requireNacos(infra.namingA(), nacosService(dubboInterface, dubboGroup),
                    dubboGroup, ip, DUBBO_PORT).getMetadata();
            assertThat(metadata).containsEntry("interface", dubboInterface).containsEntry("version", VERSION)
                    .containsEntry("group", dubboGroup).containsEntry("protocol", "dubbo");
            assertSyncMarkers(metadata, cluster.zookeeper(), cluster.nacosA(), "ZK");
        });
    }

    @Test
    void removedDubboProviderLeavesNacos() throws Exception {
        String dubboInterface = uniqueInterface("ZkRemoval");
        String dubboGroup = uniqueName("ZK-GROUP");
        String ip = randomIp();
        String providerPath = createDubboProvider(dubboInterface, dubboGroup, ip);
        addTask(cluster.zookeeper(), cluster.nacosA(), dubboInterface, dubboGroup, VERSION);
        String nacosService = nacosService(dubboInterface, dubboGroup);
        eventually().untilAsserted(() -> requireNacos(infra.namingA(), nacosService, dubboGroup, ip, DUBBO_PORT));

        infra.curator().delete().forPath(providerPath);

        eventually().untilAsserted(
                () -> assertThat(findNacos(infra.namingA(), nacosService, dubboGroup, ip, DUBBO_PORT)).isEmpty());
    }

    @Test
    void everyProviderOfAnInterfaceIsSyncedToNacos() throws Exception {
        String dubboInterface = uniqueInterface("ZkProviders");
        String dubboGroup = uniqueName("ZK-GROUP");
        String firstIp = randomIp();
        String secondIp = randomIp();
        createDubboProvider(dubboInterface, dubboGroup, firstIp);
        addTask(cluster.zookeeper(), cluster.nacosA(), dubboInterface, dubboGroup, VERSION);
        String nacosService = nacosService(dubboInterface, dubboGroup);
        eventually().untilAsserted(() -> requireNacos(infra.namingA(), nacosService, dubboGroup, firstIp, DUBBO_PORT));

        createDubboProvider(dubboInterface, dubboGroup, secondIp);

        String providers = "first provider " + firstIp + ", second provider " + secondIp;
        eventually().untilAsserted(() -> assertThat(nacosInstances(infra.namingA(), nacosService, dubboGroup))
                .extracting(Instance::getIp).as(providers).containsExactlyInAnyOrder(firstIp, secondIp));
        stable().untilAsserted(() -> assertThat(nacosInstances(infra.namingA(), nacosService, dubboGroup))
                .extracting(Instance::getIp).as(providers).containsExactlyInAnyOrder(firstIp, secondIp));
    }

    @Test
    void nacosInstanceIsSyncedToZookeeperAndRemovedWithIt() throws Exception {
        String service = uniqueName("N2ZK");
        String dubboInterface = uniqueInterface("NacosToZk");
        String ip = randomIp();
        // nacos-sync builds the Dubbo URL (and the providers path) from these metadata keys.
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, DUBBO_PORT,
                Map.of("interface", dubboInterface, "protocol", "dubbo", "version", VERSION, "side", "provider"));
        addTask(cluster.nacosA(), cluster.zookeeper(), service, DEFAULT_GROUP);
        String providersPath = "/dubbo/" + dubboInterface + "/providers";

        eventually().untilAsserted(() -> assertThat(providerUrls(providersPath)).singleElement().satisfies(url -> {
            assertThat(url).startsWith("dubbo://" + ip + ":" + DUBBO_PORT + "/" + dubboInterface + "?");
            assertThat(url).contains("sourceClusterId=" + cluster.nacosA(), "destClusterId=" + cluster.zookeeper(),
                    "syncSource=NACOS");
        }));

        infra.namingA().deregisterInstance(service, DEFAULT_GROUP, ip, DUBBO_PORT);
        eventually().untilAsserted(() -> assertThat(providerUrls(providersPath)).isEmpty());
    }

    /** Creates an ephemeral provider node the way a Dubbo provider registers itself. */
    private String createDubboProvider(String dubboInterface, String dubboGroup, String ip) throws Exception {
        String url = "dubbo://" + ip + ":" + DUBBO_PORT + "/" + dubboInterface + "?anyhost=true"
                + "&application=nacos-sync-it-provider&dubbo=2.0.2&group=" + dubboGroup + "&interface="
                + dubboInterface + "&methods=sayHello&release=2.7.23&side=provider&version=" + VERSION;
        String path = "/dubbo/" + dubboInterface + "/providers/" + URLEncoder.encode(url, StandardCharsets.UTF_8);
        infra.curator().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
        afterTest(() -> infra.curator().delete().quietly().forPath(path));
        return path;
    }

    private List<String> providerUrls(String providersPath) throws Exception {
        return infra.curator().getChildren().forPath(providersPath).stream()
                .map(child -> URLDecoder.decode(child, StandardCharsets.UTF_8)).toList();
    }

    /** Nacos service name nacos-sync derives for a Dubbo provider with a group. */
    private static String nacosService(String dubboInterface, String dubboGroup) {
        return "providers:" + dubboInterface + ":" + VERSION + ":" + dubboGroup;
    }

    private static String uniqueInterface(String simpleName) {
        return "com.alibaba.nacossync.it." + simpleName + uniqueName("").replace("-", "");
    }
}
