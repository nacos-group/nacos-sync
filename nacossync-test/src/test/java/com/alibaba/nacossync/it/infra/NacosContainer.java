package com.alibaba.nacossync.it.infra;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

import static org.awaitility.Awaitility.await;

/**
 * Standalone Nacos server with auth enabled, for image tags v3.x (default) and v2.x.
 *
 * <p>nacos-client always dials gRPC on the HTTP port + 1000, which random Testcontainers mappings break. The HTTP
 * and gRPC ports are therefore bound to a fixed host pair P / P + 1000, with P taken from the reserved range in
 * {@link HostPorts} (40000-40999 by default, so gRPC uses 41000-41999).
 */
public final class NacosContainer extends GenericContainer<NacosContainer> {

    /** Test-only credentials. Nothing here is a real secret. */
    public static final String USERNAME = "nacos";

    public static final String PASSWORD = "nacos-sync-it-password";

    // Nacos requires the decoded JWT secret to be at least 32 bytes.
    private static final String AUTH_TOKEN = Base64.getEncoder()
            .encodeToString("nacos-sync-it-jwt-secret-for-tests-only".getBytes(StandardCharsets.UTF_8));

    private static final String IDENTITY_KEY = "nacos-sync-it-identity";

    private static final String IDENTITY_VALUE = "nacos-sync-it-identity-value";

    private static final int HTTP_PORT = 8848;

    private static final int GRPC_PORT = 9848;

    private static final int GRPC_PORT_OFFSET = GRPC_PORT - HTTP_PORT;

    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

    private final boolean v2Api;

    private final int hostHttpPort;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    private NacosContainer(String imageTag, int hostHttpPort) {
        super(DockerImageName.parse("nacos/nacos-server").withTag(imageTag));
        this.v2Api = imageTag.startsWith("v2.");
        this.hostHttpPort = hostHttpPort;
        withEnv("MODE", "standalone");
        withEnv("NACOS_AUTH_ENABLE", "true");
        withEnv("NACOS_AUTH_TOKEN", AUTH_TOKEN);
        withEnv("NACOS_AUTH_IDENTITY_KEY", IDENTITY_KEY);
        withEnv("NACOS_AUTH_IDENTITY_VALUE", IDENTITY_VALUE);
        // The image defaults to a 1g heap; two servers plus the rest of the stack do not need that.
        withEnv("JVM_XMS", "256m");
        withEnv("JVM_XMX", "512m");
        withEnv("JVM_XMN", "128m");
        withExposedPorts(HTTP_PORT, GRPC_PORT);
        setPortBindings(List.of(hostHttpPort + ":" + HTTP_PORT, (hostHttpPort + GRPC_PORT_OFFSET) + ":" + GRPC_PORT));
        waitingFor(Wait.forHttp(readinessPath()).forPort(HTTP_PORT).forStatusCode(200)
                .forResponsePredicate(NacosContainer::isReadyBody).withStartupTimeout(STARTUP_TIMEOUT));
    }

    /**
     * Starts a server on a free pair from the reserved range, retrying with the next pair if another process grabbed
     * one of the ports first.
     */
    static NacosContainer startNew(String imageTag) {
        RuntimeException lastFailure = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            NacosContainer container = new NacosContainer(imageTag, HostPorts.freeNacosPortPair(GRPC_PORT_OFFSET));
            try {
                container.start();
                return container;
            } catch (RuntimeException e) {
                lastFailure = e;
                container.stop();
            }
        }
        throw lastFailure;
    }

    /** Address in the form nacos-client and the nacos-sync cluster config expect. */
    public String serverAddr() {
        return getHost() + ":" + hostHttpPort;
    }

    public NamingService newNamingService() throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverAddr());
        properties.setProperty(PropertyKeyConst.USERNAME, USERNAME);
        properties.setProperty(PropertyKeyConst.PASSWORD, PASSWORD);
        return NamingFactory.createNamingService(properties);
    }

    /** docker stop + docker start of the same container; data and port bindings survive, registrations do not. */
    public void restart() {
        getDockerClient().stopContainerCmd(getContainerId()).withTimeout(10).exec();
        getDockerClient().startContainerCmd(getContainerId()).exec();
        awaitReady();
    }

    /** Freezes the server process, which looks like a network partition to every client. */
    public void pause() {
        getDockerClient().pauseContainerCmd(getContainerId()).exec();
    }

    public void unpause() {
        getDockerClient().unpauseContainerCmd(getContainerId()).exec();
        awaitReady();
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        initAdminPassword();
    }

    private void awaitReady() {
        await("Nacos " + serverAddr() + " readiness").atMost(STARTUP_TIMEOUT).pollInterval(Duration.ofSeconds(1))
                .ignoreExceptions().until(() -> {
                    HttpResponse<String> response = send(HttpRequest.newBuilder(uri(readinessPath())).GET());
                    return response.statusCode() == 200 && isReadyBody(response.body());
                });
    }

    /** Nacos 2.4+ starts without an admin password; the first call to this endpoint sets it. */
    private void initAdminPassword() {
        String path = v2Api ? "/nacos/v1/auth/users/admin" : "/nacos/v3/auth/user/admin";
        HttpRequest.Builder request = HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("password=" + PASSWORD));
        try {
            HttpResponse<String> response = send(request);
            if (response.statusCode() != 200 || !response.body().contains(USERNAME)) {
                throw new IllegalStateException(
                        "Nacos admin init failed: HTTP " + response.statusCode() + " " + response.body());
            }
        } catch (IOException e) {
            throw new IllegalStateException("Nacos admin init failed", e);
        }
    }

    private String readinessPath() {
        return v2Api ? "/nacos/v1/console/health/readiness" : "/nacos/v3/admin/core/state/readiness";
    }

    /** v3 answers HTTP 200 with a non-zero code while naming is still starting; v2 answers a plain "OK". */
    private static boolean isReadyBody(String body) {
        return body.contains("\"code\":0") || "OK".equals(body.trim());
    }

    private URI uri(String path) {
        return URI.create("http://" + serverAddr() + path);
    }

    private HttpResponse<String> send(HttpRequest.Builder request) throws IOException {
        try {
            return http.send(request.timeout(Duration.ofSeconds(5)).build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
