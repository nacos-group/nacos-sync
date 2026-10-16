package com.alibaba.nacossync.it.infra;

import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

import java.time.Duration;

import static org.awaitility.Awaitility.await;

/**
 * Eureka server running in the test JVM as its own Spring context, next to (not inside) the nacos-sync context.
 */
final class EmbeddedEurekaServer {

    private final int port;

    private EmbeddedEurekaServer(int port) {
        this.port = port;
    }

    static EmbeddedEurekaServer start() {
        int port = HostPorts.ephemeral();
        String selfUrl = "http://localhost:" + port + "/eureka/";
        // Command-line args take precedence over any application.properties on the test classpath.
        new SpringApplicationBuilder(EurekaServerApp.class)
                .bannerMode(Banner.Mode.OFF)
                .run("--spring.config.name=nacos-sync-it-eureka",
                        "--spring.application.name=nacos-sync-it-eureka",
                        "--server.port=" + port,
                        "--eureka.instance.hostname=localhost",
                        "--eureka.client.register-with-eureka=false",
                        "--eureka.client.fetch-registry=false",
                        // Pointing the peer list at itself leaves it empty: no replication attempts to :8761.
                        "--eureka.client.service-url.defaultZone=" + selfUrl,
                        "--eureka.server.enable-self-preservation=false",
                        "--eureka.server.wait-time-in-ms-when-sync-empty=0",
                        "--eureka.server.registry-sync-retries=0",
                        // Serve reads from the write-invalidated cache instead of the 30 s read-only snapshot.
                        "--eureka.server.use-read-only-response-cache=false",
                        "--eureka.dashboard.enabled=false",
                        "--spring.jmx.enabled=false");
        EmbeddedEurekaServer server = new EmbeddedEurekaServer(port);
        EurekaTestClient client = new EurekaTestClient(server.serviceUrl());
        // The registry is initialised on a background thread after the web server is up.
        await("embedded Eureka server").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().until(client::isUp);
        return server;
    }

    /** Service URL in the form nacos-sync expects for an EUREKA cluster. */
    String serviceUrl() {
        return "http://localhost:" + port + "/eureka";
    }

    /**
     * Deliberately not a {@code @Configuration}: nacos-sync's component scan covers {@code com.alibaba.nacossync}, and
     * a scanned Eureka server config would start a second registry inside the nacos-sync context. JDBC/JPA come with
     * the worker on the classpath and are not needed here.
     */
    @EnableEurekaServer
    @EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class,
            DataSourceTransactionManagerAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
    static class EurekaServerApp {

    }
}
