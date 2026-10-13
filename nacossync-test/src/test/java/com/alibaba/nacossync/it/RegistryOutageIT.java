package com.alibaba.nacossync.it;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

/**
 * Destination outages on a Nacos A -> Nacos B task. The synced instances must come back once Nacos B is reachable
 * again, without waiting for the 300 s full re-sync. That they then stay put is
 * {@link NacosNacosIT#syncedInstanceStaysRegisteredWhileTaskRuns()}.
 *
 * <p>Nacos B is shared with other test classes; both tests leave it running and ready.
 */
class RegistryOutageIT extends AbstractSyncIT {

    private static final int PORT = 8080;

    /** Long enough for the clients' gRPC health checks to notice the connection is gone. */
    private static final Duration PARTITION = Duration.ofSeconds(20);

    @Test
    void syncedInstanceComesBackAfterDestinationRestart() throws Exception {
        String service = uniqueName("OUTAGE-RESTART");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of());
        addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> requireNacos(infra.namingB(), service, DEFAULT_GROUP, ip, PORT));

        // A restarted Nacos keeps its data but loses every ephemeral registration.
        infra.nacosB().restart();

        eventually().untilAsserted(() -> requireNacos(infra.namingB(), service, DEFAULT_GROUP, ip, PORT));
    }

    @Test
    void syncedInstanceComesBackAfterDestinationPartition() throws Exception {
        String service = uniqueName("OUTAGE-PAUSE");
        String ip = randomIp();
        registerNacos(infra.namingA(), service, DEFAULT_GROUP, ip, PORT, Map.of());
        addTask(cluster.nacosA(), cluster.nacosB(), service, DEFAULT_GROUP);
        eventually().untilAsserted(() -> requireNacos(infra.namingB(), service, DEFAULT_GROUP, ip, PORT));

        infra.nacosB().pause();
        try {
            Thread.sleep(PARTITION.toMillis());
        } finally {
            infra.nacosB().unpause();
        }

        eventually().untilAsserted(() -> requireNacos(infra.namingB(), service, DEFAULT_GROUP, ip, PORT));
    }
}
