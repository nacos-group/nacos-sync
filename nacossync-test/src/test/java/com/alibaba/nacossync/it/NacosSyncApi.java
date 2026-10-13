package com.alibaba.nacossync.it;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.util.DigestUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Client for the nacos-sync REST API. Every call is HTTP 200; failures are reported in the {@code success} field, so
 * each call asserts on it.
 */
final class NacosSyncApi {

    private final RestTemplate rest = new RestTemplate();

    private final ObjectMapper json = new ObjectMapper();

    private final String baseUrl;

    NacosSyncApi(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /** nacos-sync does not return the id; it is MD5(clusterName + "_" + clusterType). */
    static String clusterId(String clusterName, String clusterType) {
        return md5(clusterName + "_" + clusterType);
    }

    /** MD5(serviceName_groupName_sourceClusterId_destClusterId). */
    static String taskId(String serviceName, String groupName, String sourceClusterId, String destClusterId) {
        return md5(String.join("_", serviceName, groupName, sourceClusterId, destClusterId));
    }

    /** Adds the cluster unless a cluster with the same id already exists, and returns its id. */
    String ensureCluster(String clusterName, String clusterType, List<String> connectKeys, String userName,
            String password) {
        String clusterId = clusterId(clusterName, clusterType);
        if (clusterIds().contains(clusterId)) {
            return clusterId;
        }
        ObjectNode body = json.createObjectNode();
        body.put("clusterName", clusterName);
        body.put("clusterType", clusterType);
        connectKeys.forEach(body.putArray("connectKeyList")::add);
        body.put("userName", userName);
        body.put("password", password);
        call(HttpMethod.POST, "/v1/cluster/add", body);
        return clusterId;
    }

    String addTask(String sourceClusterId, String destClusterId, String serviceName, String groupName,
            String version) {
        ObjectNode body = json.createObjectNode();
        body.put("sourceClusterId", sourceClusterId);
        body.put("destClusterId", destClusterId);
        body.put("serviceName", serviceName);
        body.put("groupName", groupName);
        body.put("version", version);
        call(HttpMethod.POST, "/v1/task/add", body);
        return taskId(serviceName, groupName, sourceClusterId, destClusterId);
    }

    /** SYNC resumes a task; DELETE pauses it (synced instances are removed, the task row is kept). */
    void updateTaskStatus(String taskId, String taskStatus) {
        ObjectNode body = json.createObjectNode();
        body.put("taskId", taskId);
        body.put("taskStatus", taskStatus);
        call(HttpMethod.POST, "/v1/task/update", body);
    }

    void deleteTask(String taskId) {
        call(HttpMethod.DELETE, UriComponentsBuilder.fromPath("/v1/task/delete").queryParam("taskId", taskId)
                .toUriString(), null);
    }

    void deleteTasksInBatch(List<String> taskIds) {
        call(HttpMethod.DELETE, UriComponentsBuilder.fromPath("/v1/task/deleteInBatch")
                .queryParam("taskIds", taskIds.toArray()).toUriString(), null);
    }

    List<String> taskIds() {
        JsonNode result = call(HttpMethod.GET, "/v1/task/list?pageNum=1&pageSize=1000", null);
        List<String> ids = new ArrayList<>();
        result.path("taskModels").forEach(task -> ids.add(task.path("taskId").asText()));
        return ids;
    }

    private List<String> clusterIds() {
        JsonNode result = call(HttpMethod.GET, "/v1/cluster/list?pageNum=1&pageSize=1000", null);
        List<String> ids = new ArrayList<>();
        result.path("clusterModels").forEach(cluster -> ids.add(cluster.path("clusterId").asText()));
        return ids;
    }

    private JsonNode call(HttpMethod method, String pathAndQuery, JsonNode body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        String requestBody = body == null ? null : body.toString();
        String response = rest.exchange(baseUrl + pathAndQuery, method, new HttpEntity<>(requestBody, headers),
                String.class).getBody();
        try {
            JsonNode result = json.readTree(response);
            if (!result.path("success").asBoolean(false)) {
                throw new AssertionError(method + " " + pathAndQuery + " failed: " + response);
            }
            return result;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unparseable nacos-sync response: " + response, e);
        }
    }

    private static String md5(String value) {
        return DigestUtils.md5DigestAsHex(value.getBytes(StandardCharsets.UTF_8));
    }
}
