package com.alibaba.nacossync.it.infra;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Minimal Eureka REST client (JSON) that plays the role of applications registering with Eureka.
 */
public final class EurekaTestClient {

    // Long enough that test instances are never evicted for lack of heartbeats.
    private static final int LEASE_DURATION_SECONDS = 600;

    private final RestTemplate rest = new RestTemplate();

    private final ObjectMapper json = new ObjectMapper();

    private final String serviceUrl;

    EurekaTestClient(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public static String instanceId(String app, String ip, int port) {
        return ip + ":" + app.toLowerCase() + ":" + port;
    }

    /** Registers an UP instance and returns its instance id. */
    public String register(String app, String ip, int port, Map<String, String> metadata) {
        String instanceId = instanceId(app, ip, port);
        ObjectNode instance = json.createObjectNode();
        instance.put("instanceId", instanceId);
        instance.put("hostName", ip);
        instance.put("app", app);
        instance.put("ipAddr", ip);
        instance.put("status", "UP");
        instance.put("vipAddress", app.toLowerCase());
        instance.putObject("port").put("$", port).put("@enabled", "true");
        instance.putObject("securePort").put("$", 443).put("@enabled", "false");
        instance.putObject("dataCenterInfo").put("@class", "com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo")
                .put("name", "MyOwn");
        instance.putObject("leaseInfo").put("renewalIntervalInSecs", 30)
                .put("durationInSecs", LEASE_DURATION_SECONDS);
        ObjectNode metadataNode = instance.putObject("metadata");
        metadata.forEach(metadataNode::put);
        ObjectNode body = json.createObjectNode();
        body.set("instance", instance);
        rest.exchange(serviceUrl + "/apps/{app}", HttpMethod.POST, new HttpEntity<>(body.toString(), jsonHeaders()),
                Void.class, app);
        return instanceId;
    }

    /** Sets the overridden status, e.g. OUT_OF_SERVICE or UP. */
    public void setStatus(String app, String instanceId, String status) {
        rest.put(serviceUrl + "/apps/{app}/{id}/status?value={status}", null, app, instanceId, status);
    }

    public void putMetadata(String app, String instanceId, String key, String value) {
        rest.put(serviceUrl + "/apps/{app}/{id}/metadata?{key}={value}", null, app, instanceId, key, value);
    }

    public void deregister(String app, String instanceId) {
        rest.delete(serviceUrl + "/apps/{app}/{id}", app, instanceId);
    }

    /** All instances of an application as Eureka JSON nodes; empty when the application is unknown. */
    public List<JsonNode> instances(String app) {
        JsonNode application;
        try {
            String body = rest.exchange(serviceUrl + "/apps/{app}", HttpMethod.GET, new HttpEntity<>(jsonHeaders()),
                    String.class, app).getBody();
            application = json.readTree(body).path("application");
        } catch (HttpClientErrorException.NotFound e) {
            return List.of();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read Eureka application " + app, e);
        }
        JsonNode instance = application.path("instance");
        List<JsonNode> result = new ArrayList<>();
        if (instance.isArray()) {
            instance.forEach(result::add);
        } else if (instance.isObject()) {
            result.add(instance);
        }
        return result;
    }

    public Optional<JsonNode> find(String app, String ip, int port) {
        return instances(app).stream()
                .filter(i -> ip.equals(i.path("ipAddr").asText()) && port == i.path("port").path("$").asInt())
                .findFirst();
    }

    boolean isUp() {
        return rest.exchange(serviceUrl + "/apps", HttpMethod.GET, new HttpEntity<>(jsonHeaders()), String.class)
                .getStatusCode().is2xxSuccessful();
    }

    private static HttpHeaders jsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        return headers;
    }
}
