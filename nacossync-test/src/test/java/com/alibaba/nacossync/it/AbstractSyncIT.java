package com.alibaba.nacossync.it;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacossync.NacosSyncMain;
import com.alibaba.nacossync.it.infra.NacosContainer;
import com.alibaba.nacossync.it.infra.SharedInfrastructure;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.mysql.MySQLContainer;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs nacos-sync in-process against the shared containers. All subclasses use the same Spring configuration, so
 * one nacos-sync context (and one set of clusters) serves the whole JVM.
 *
 * <p>Every test uses its own uniquely named services and cleans up the tasks and instances it created, so tests do
 * not depend on each other or on execution order.
 */
@SpringBootTest(classes = NacosSyncMain.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                // application.properties is not packaged in the worker jar; repeat the settings nacos-sync needs.
                "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.MySQLDialect",
                "spring.jpa.hibernate.ddl-auto=update",
                "spring.cloud.discovery.enabled=false",
                "eureka.client.enabled=false"})
abstract class AbstractSyncIT {

    static final String DEFAULT_GROUP = "DEFAULT_GROUP";

    static final String STATUS_SYNC = "SYNC";

    /** Task status that pauses a task: synced instances are removed but the task row is kept. */
    static final String STATUS_PAUSED = "DELETE";

    static final String SOURCE_CLUSTER_ID = "sourceClusterId";

    static final String DEST_CLUSTER_ID = "destClusterId";

    static final String SYNC_SOURCE = "syncSource";

    static final String SYNC_INSTANCE_TAG = "sync.instance.tag";

    /** Covers the 3 s task/poll timers several times over without waiting for the 300 s full re-sync. */
    static final Duration SYNC_TIMEOUT = Duration.ofSeconds(90);

    /** How long a state must hold to count as stable (no echo, no flapping). */
    static final Duration STABLE_PERIOD = Duration.ofSeconds(20);

    private static final Logger log = LoggerFactory.getLogger(AbstractSyncIT.class);

    private static Clusters clusters;

    @DynamicPropertySource
    static void nacosSyncDatabase(DynamicPropertyRegistry registry) {
        MySQLContainer mysql = SharedInfrastructure.get().mysql();
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
    }

    protected final SharedInfrastructure infra = SharedInfrastructure.get();

    protected NacosSyncApi api;

    protected Clusters cluster;

    private final Deque<Cleanup> cleanups = new ArrayDeque<>();

    @LocalServerPort
    private int port;

    @Autowired
    private ApplicationContext nacosSyncContext;

    @BeforeEach
    void connectNacosSync() {
        assertThat(nacosSyncContext.getBeanNamesForType(PeerAwareInstanceRegistry.class))
                .as("the embedded Eureka server must not run inside the nacos-sync context").isEmpty();
        api = new NacosSyncApi("http://localhost:" + port);
        cluster = ensureClusters(api, infra);
    }

    @AfterEach
    void runCleanups() {
        while (!cleanups.isEmpty()) {
            try {
                cleanups.pop().run();
            } catch (Exception | AssertionError e) {
                log.warn("Cleanup step failed", e);
            }
        }
    }

    /** Registered cleanups run after the test, last registered first. */
    protected void afterTest(Cleanup cleanup) {
        cleanups.push(cleanup);
    }

    // ---------------------------------------------------------------- nacos-sync tasks

    protected String addTask(String sourceClusterId, String destClusterId, String serviceName, String groupName) {
        return addTask(sourceClusterId, destClusterId, serviceName, groupName, null);
    }

    /** Adds a sync task that is deleted after the test unless the test already deleted it. */
    protected String addTask(String sourceClusterId, String destClusterId, String serviceName, String groupName,
            String version) {
        String taskId = api.addTask(sourceClusterId, destClusterId, serviceName, groupName, version);
        afterTest(() -> {
            if (api.taskIds().contains(taskId)) {
                api.deleteTask(taskId);
            }
        });
        return taskId;
    }

    // ---------------------------------------------------------------- Nacos

    /** A second client for the same server, as a separate application process would have. Shut down after the test. */
    protected NamingService newNacosClient(NacosContainer nacos) throws NacosException {
        NamingService naming = nacos.newNamingService();
        afterTest(naming::shutDown);
        return naming;
    }

    /** Registers an ephemeral instance (one per service and client) and deregisters it after the test. */
    protected Instance registerNacos(NamingService naming, String serviceName, String groupName, String ip, int port,
            Map<String, String> metadata) throws NacosException {
        Instance instance = nacosInstance(ip, port, metadata);
        naming.registerInstance(serviceName, groupName, instance);
        afterTest(() -> naming.deregisterInstance(serviceName, groupName, ip, port));
        return instance;
    }

    protected static Instance nacosInstance(String ip, int port, Map<String, String> metadata) {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setHealthy(true);
        instance.setEnabled(true);
        instance.setMetadata(new HashMap<>(metadata));
        return instance;
    }

    /** Reads straight from the server, bypassing the client-side subscription cache. */
    protected static List<Instance> nacosInstances(NamingService naming, String serviceName, String groupName)
            throws NacosException {
        return naming.getAllInstances(serviceName, groupName, false);
    }

    protected static Optional<Instance> findNacos(NamingService naming, String serviceName, String groupName,
            String ip, int port) throws NacosException {
        return nacosInstances(naming, serviceName, groupName).stream()
                .filter(i -> ip.equals(i.getIp()) && port == i.getPort()).findFirst();
    }

    /** The instance, or an AssertionError listing what the service currently has (retried by {@link #eventually()}). */
    protected static Instance requireNacos(NamingService naming, String serviceName, String groupName, String ip,
            int port) throws NacosException {
        List<Instance> instances = nacosInstances(naming, serviceName, groupName);
        return instances.stream().filter(i -> ip.equals(i.getIp()) && port == i.getPort()).findFirst().orElseThrow(
                () -> new AssertionError(ip + ":" + port + " not in " + groupName + "@@" + serviceName + ": "
                        + instances));
    }

    // ---------------------------------------------------------------- assertions

    /** Polls until the assertion passes or {@link #SYNC_TIMEOUT} elapses. */
    protected static ConditionFactory eventually() {
        return Awaitility.await().atMost(SYNC_TIMEOUT).pollInterval(Duration.ofMillis(500)).ignoreExceptions();
    }

    /** The assertion has to hold continuously for {@link #STABLE_PERIOD}. */
    protected static ConditionFactory stable() {
        return Awaitility.await().during(STABLE_PERIOD).atMost(STABLE_PERIOD.plusSeconds(15))
                .pollInterval(Duration.ofSeconds(1));
    }

    protected static void assertSyncMarkers(Map<String, String> metadata, String sourceClusterId,
            String destClusterId, String syncSource) {
        assertThat(metadata).containsEntry(SOURCE_CLUSTER_ID, sourceClusterId)
                .containsEntry(DEST_CLUSTER_ID, destClusterId).containsEntry(SYNC_SOURCE, syncSource);
    }

    protected static void assertNoSyncMarkers(Map<String, String> metadata) {
        assertThat(metadata).doesNotContainKeys(SOURCE_CLUSTER_ID, DEST_CLUSTER_ID, SYNC_SOURCE, SYNC_INSTANCE_TAG);
    }

    // ---------------------------------------------------------------- naming

    /** Upper-case so the same name works as a Eureka application name. */
    protected static String uniqueName(String prefix) {
        return "IT-" + prefix + "-" + Integer.toHexString(ThreadLocalRandom.current().nextInt(0x10000000, 0x7fffffff))
                .toUpperCase();
    }

    protected static String randomIp() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return "10." + random.nextInt(0, 256) + "." + random.nextInt(0, 256) + "." + random.nextInt(1, 255);
    }

    private static synchronized Clusters ensureClusters(NacosSyncApi api, SharedInfrastructure infra) {
        if (clusters == null) {
            clusters = new Clusters(
                    api.ensureCluster("it-nacos-a", "NACOS", List.of(infra.nacosA().serverAddr()),
                            NacosContainer.USERNAME, NacosContainer.PASSWORD),
                    api.ensureCluster("it-nacos-b", "NACOS", List.of(infra.nacosB().serverAddr()),
                            NacosContainer.USERNAME, NacosContainer.PASSWORD),
                    api.ensureCluster("it-eureka", "EUREKA", List.of(infra.eurekaServiceUrl()), null, null),
                    api.ensureCluster("it-consul", "CONSUL", List.of(infra.consulAddress()), null, null),
                    api.ensureCluster("it-zookeeper", "ZK", List.of(infra.zookeeperAddress()), null, null));
        }
        return clusters;
    }

    /** nacos-sync cluster ids, created once per JVM. */
    record Clusters(String nacosA, String nacosB, String eureka, String consul, String zookeeper) {

    }

    @FunctionalInterface
    interface Cleanup {

        void run() throws Exception;
    }
}
