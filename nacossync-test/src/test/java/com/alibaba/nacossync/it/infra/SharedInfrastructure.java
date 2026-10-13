package com.alibaba.nacossync.it.infra;

import com.alibaba.nacos.api.naming.NamingService;
import com.ecwid.consul.v1.ConsulClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Registries and database shared by every integration test in the JVM. Started once on first use and torn down by
 * Ryuk when the JVM exits.
 *
 * <p>The Nacos image tag comes from the {@code nacos.image.tag} system property (default {@value #DEFAULT_NACOS_TAG};
 * {@code v2.5.4} is also supported).
 */
public final class SharedInfrastructure {

    public static final String DEFAULT_NACOS_TAG = "v3.2.4";

    /** Test-only database credentials. */
    private static final String MYSQL_USER = "nacos_sync";

    private static final String MYSQL_PASSWORD = "nacos-sync-it-db-password";

    private static final int CONSUL_PORT = 8500;

    private static final int ZOOKEEPER_PORT = 2181;

    private static SharedInfrastructure instance;

    private static RuntimeException startupFailure;

    private final MySQLContainer mysql;

    private final NacosContainer nacosA;

    private final NacosContainer nacosB;

    private final GenericContainer<?> consul;

    private final GenericContainer<?> zookeeper;

    private final EmbeddedEurekaServer eurekaServer;

    private final NamingService namingA;

    private final NamingService namingB;

    private final ConsulClient consulClient;

    private final CuratorFramework curator;

    private final EurekaTestClient eurekaClient;

    private SharedInfrastructure() throws Exception {
        String nacosTag = System.getProperty("nacos.image.tag", DEFAULT_NACOS_TAG);
        mysql = new MySQLContainer(DockerImageName.parse("mysql:8.4")).withDatabaseName("nacos_sync")
                .withUsername(MYSQL_USER).withPassword(MYSQL_PASSWORD);
        consul = new GenericContainer<>(DockerImageName.parse("hashicorp/consul:1.20"))
                .withCommand("agent", "-dev", "-client=0.0.0.0").withExposedPorts(CONSUL_PORT)
                .waitingFor(Wait.forHttp("/v1/status/leader").forPort(CONSUL_PORT)
                        .forResponsePredicate(leader -> leader.contains(":")));
        zookeeper = new GenericContainer<>(DockerImageName.parse("zookeeper:3.7.2-jre-17"))
                .withExposedPorts(ZOOKEEPER_PORT).waitingFor(Wait.forListeningPort());

        CompletableFuture<NacosContainer> nacosAStart = CompletableFuture.supplyAsync(
                () -> NacosContainer.startNew(nacosTag));
        CompletableFuture<NacosContainer> nacosBStart = CompletableFuture.supplyAsync(
                () -> NacosContainer.startNew(nacosTag));
        CompletableFuture<EmbeddedEurekaServer> eurekaStart = CompletableFuture.supplyAsync(
                EmbeddedEurekaServer::start);
        Startables.deepStart(mysql, consul, zookeeper).join();
        nacosA = nacosAStart.join();
        nacosB = nacosBStart.join();
        eurekaServer = eurekaStart.join();

        namingA = nacosA.newNamingService();
        namingB = nacosB.newNamingService();
        consulClient = new ConsulClient(consul.getHost(), consul.getMappedPort(CONSUL_PORT));
        curator = CuratorFrameworkFactory.newClient(zookeeperAddress(), new ExponentialBackoffRetry(500, 5));
        curator.start();
        if (!curator.blockUntilConnected(60, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Zookeeper not reachable at " + zookeeperAddress());
        }
        eurekaClient = new EurekaTestClient(eurekaServer.serviceUrl());
    }

    public static synchronized SharedInfrastructure get() {
        if (startupFailure != null) {
            throw startupFailure;
        }
        if (instance == null) {
            try {
                instance = new SharedInfrastructure();
            } catch (Exception e) {
                startupFailure = new IllegalStateException("Integration test infrastructure failed to start", e);
                throw startupFailure;
            }
        }
        return instance;
    }

    public MySQLContainer mysql() {
        return mysql;
    }

    public NacosContainer nacosA() {
        return nacosA;
    }

    public NacosContainer nacosB() {
        return nacosB;
    }

    public NamingService namingA() {
        return namingA;
    }

    public NamingService namingB() {
        return namingB;
    }

    public String consulAddress() {
        return consul.getHost() + ":" + consul.getMappedPort(CONSUL_PORT);
    }

    public ConsulClient consul() {
        return consulClient;
    }

    public String zookeeperAddress() {
        return zookeeper.getHost() + ":" + zookeeper.getMappedPort(ZOOKEEPER_PORT);
    }

    public CuratorFramework curator() {
        return curator;
    }

    public String eurekaServiceUrl() {
        return eurekaServer.serviceUrl();
    }

    public EurekaTestClient eureka() {
        return eurekaClient;
    }
}
