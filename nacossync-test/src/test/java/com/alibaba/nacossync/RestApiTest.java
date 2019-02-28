package com.alibaba.nacossync;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacossync.util.HttpClient;
import com.alibaba.nacossync.util.HttpClient.HttpResult;

import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Created by mingyi.xxc
 * Date: 2019/2/23
 * Time: 下午7:20
 * DESC:
 *
 * @author mingyi.xxc
 * @date 2019/02/23
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = NacosSyncMain.class, properties = {"server.servlet.context-path=/",
    "server.port=8081"},
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class RestApiTest {

    private String baseUrl;

    @LocalServerPort
    private int port;

    @Before
    public void setUp() throws Exception {
        this.baseUrl = String.format("http://localhost:%d", port);
    }

    @After
    public void cleanup() throws Exception {

    }

    @Test
    public void addCluster() {
        String url = baseUrl + "/v1/cluster/add";

        JSONObject clusterJson = new JSONObject();
        clusterJson.put("clusterName", "CI-Nacos-Test" + System.currentTimeMillis());
        clusterJson.put("clusterType", "NACOS");

        JSONArray jsonArray = new JSONArray();
        jsonArray.add("11.11.11.11");
        jsonArray.add("22.22.22.22");
        clusterJson.put("connectKeyList", jsonArray);

        try {
            HttpClient.HttpResult result = HttpClient.httpPost(url, clusterJson.toJSONString());
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void addClusterNotHaveClusterType() {
        String url = baseUrl + "/v1/cluster/add";

        JSONObject clusterJson = new JSONObject();
        clusterJson.put("clusterName", "CI-Nacos-Test" + System.currentTimeMillis());

        JSONArray jsonArray = new JSONArray();
        jsonArray.add("11.11.11.11");
        jsonArray.add("22.22.22.22");
        clusterJson.put("connectKeyList", jsonArray);

        try {
            HttpClient.HttpResult result = HttpClient.httpPost(url, clusterJson.toJSONString());
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertFalse(JSON.parseObject(result.content).getBoolean("success"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void deleteCluster() {
        String url = baseUrl + "/v1/cluster/list";
        String clusterId = "";
        try {
            Map<String, String> paramValues = new HashMap<>();
            paramValues.put("pageNum", "1");
            paramValues.put("pageSize", "50");

            HttpResult result = HttpClient.httpGet(url, null, paramValues);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            JSONArray clusterModels = JSON.parseObject(result.content).getJSONArray("clusterModels");
            if (clusterModels.size() > 0) {
                for (int i=0; i<clusterModels.size(); i++) {
                    if (clusterModels.getJSONObject(i).getString("clusterName").startsWith("CI-Nacos-Test")) {
                        clusterId = clusterModels.getJSONObject(i).getString("clusterId");
                        System.out.println("find cluster id = " + clusterId);
                        break;
                    }
                }
            }

            url = baseUrl + "/v1/cluster/delete";
            paramValues = new HashMap<>();
            paramValues.put("clusterId", clusterId);

            result = HttpClient.httpDelete(url, paramValues, null, "UTF-8");
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
            System.out.println(result.content);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    /**
     * delete clusterId不存在
     */
    @Test
    public void deleteClusterByClusterId() {
        String clusterId = "";
        try {
            String url = baseUrl + "/v1/cluster/delete";
            Map<String, String> paramValues = new HashMap<>();
            paramValues.put("clusterId", clusterId);

            HttpResult result = HttpClient.httpDelete(url, paramValues, null, "UTF-8");
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
            System.out.println(result.content);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void getClusterDetails() {
        addCluster();

        String clusterId = "";
        String url = baseUrl + "/v1/cluster/list";
        try {
            Map<String, String> paramValues = new HashMap<>();
            paramValues.put("pageNum", "1");
            paramValues.put("pageSize", "50");

            HttpResult result = HttpClient.httpGet(url, null, paramValues);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            JSONArray clusterModels = JSON.parseObject(result.content).getJSONArray("clusterModels");
            if (clusterModels.size() > 0) {
                for (int i=0; i<clusterModels.size(); i++) {
                    if (clusterModels.getJSONObject(i).getString("clusterName").startsWith("CI-Nacos-Test")) {
                        clusterId = clusterModels.getJSONObject(i).getString("clusterId");
                        System.out.println("find cluster id = " + clusterId);
                        break;
                    }
                }
            }

            url = baseUrl + "/v1/cluster/detail";
            paramValues = new HashMap<>();
            paramValues.put("clusterId", clusterId);

            result = HttpClient.httpGet(url, null, paramValues);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
            Assert.assertEquals(clusterId, JSON.parseObject(result.content).getJSONObject("clusterModel").getString("clusterId"));
            System.out.println(result.content);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void getClusterList() {
        String url = baseUrl + "/v1/cluster/list";
        try {
            Map<String, String> paramValues = new HashMap<>();
            paramValues.put("pageNum", "1");
            paramValues.put("pageSize", "10");

            HttpResult result = HttpClient.httpGet(url, null, paramValues);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            System.out.println(result.content);
            Assert.assertTrue(JSON.parseObject(result.content).getJSONArray("clusterModels").size() >= 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void getClusterListByClusterName() {
        String url = baseUrl + "/v1/cluster/list";
        try {
            Map<String, String> paramValues = new HashMap<>();
            paramValues.put("pageNum", "1");
            paramValues.put("pageSize", "10");
            paramValues.put("clusterName", "-CI");

            HttpResult result = HttpClient.httpGet(url, null, paramValues);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getJSONArray("clusterModels").size() == 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void getClusterTypes() {
        String url = baseUrl + "/v1/cluster/types";
        try {
            HttpResult result = HttpClient.httpGet(url, null, null);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            System.out.println(result.content);
            Assert.assertTrue(JSON.parseObject(result.content).getJSONArray("types").contains("NACOS"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void addSystemConfig() {
        String url = baseUrl + "/v1/systemconfig/add";

        JSONObject clusterJson = new JSONObject();
        clusterJson.put("config_desc", "test");
        clusterJson.put("config_key", "key");
        clusterJson.put("config_value", "value");

        try {
            HttpClient.HttpResult result = HttpClient.httpPost(url, clusterJson.toJSONString());

            System.out.println(result.content);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void deleteSystemConfig() {
        String url = baseUrl + "/v1/systemconfig/delete";
        Map<String, String> paramValues = new HashMap<>();
        paramValues.put("config_desc", "test");
        paramValues.put("config_key", "key");
        paramValues.put("config_value", "value");

        try {
            HttpClient.HttpResult result = HttpClient.httpDelete(url, paramValues, null, "UTF-8");
            //Assert.assertEquals(HttpStatus.SC_OK, result.code);
            //Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
            System.out.println(result.content);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void getSystemConfigList() {
        String url = baseUrl + "/v1/systemconfig/list";
        try {
            HttpResult result = HttpClient.httpGet(url, null, null);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            System.out.println(result.content);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void addTask() {
        addCluster("EUREKA");
        addCluster("NACOS");
        String destClusterId = getClusterId("NACOS");
        String sourceClusterId = getClusterId("EUREKA");

        String url = baseUrl + "/v1/task/add";

        JSONObject clusterJson = new JSONObject();
        clusterJson.put("destClusterId", destClusterId);
        clusterJson.put("groupName", "eureka");
        clusterJson.put("nameSpace", "CI-test");
        clusterJson.put("serviceName", "CI-Nacos-Service" + System.currentTimeMillis());
        clusterJson.put("sourceClusterId", sourceClusterId);
        clusterJson.put("version", "1.0.0");

        try {
            HttpClient.HttpResult result = HttpClient.httpPost(url, clusterJson.toJSONString());
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void deleteTask() {
        String url = baseUrl + "/v1/task/delete";
        Map<String, String> paramValues = new HashMap<>();
        paramValues.put("taskId", getTaskId());

        try {
            HttpClient.HttpResult result = HttpClient.httpDelete(url, paramValues, null, "UTF-8");
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
            System.out.println(result.content);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void deleteTaskByTaskId() {
        String url = baseUrl + "/v1/task/delete";
        Map<String, String> paramValues = new HashMap<>();
        paramValues.put("taskId", "");

        try {
            HttpClient.HttpResult result = HttpClient.httpDelete(url, paramValues, null, "UTF-8");
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
            System.out.println(result.content);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void getTaskDetail() {
        String taskId = getTaskId();
        String url = baseUrl + "/v1/task/detail";
        Map<String, String> paramValues = new HashMap<>();
        paramValues.put("taskId", taskId);

        try {
            HttpClient.HttpResult result = HttpClient.httpGet(url, null, paramValues);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
            Assert.assertEquals(taskId, JSON.parseObject(result.content).getJSONObject("taskModel").getString("taskId"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    /**
     * taskId为空, 必须存在taskId
     */
    @Test
    public void getTaskDetailWithTaskId() {
        String taskId = "";
        String url = baseUrl + "/v1/task/detail";
        Map<String, String> paramValues = new HashMap<>();
        paramValues.put("taskId", taskId);

        try {
            HttpClient.HttpResult result = HttpClient.httpGet(url, null, paramValues);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            System.out.print(result.content);
            Assert.assertFalse(JSON.parseObject(result.content).getBoolean("success"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void getTaskList() {
        String url = baseUrl + "/v1/task/list";
        try {
            Map<String, String> paramValues = new HashMap<>();
            paramValues.put("pageNum", "1");
            paramValues.put("pageSize", "10");

            HttpResult result = HttpClient.httpGet(url, null, paramValues);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);

            Assert.assertTrue(JSON.parseObject(result.content).getJSONArray("taskModels").size() >= 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void updateTask() {
        String url = baseUrl + "/v1/task/update";
        String taskId = getTaskId();
        JSONObject clusterJson = new JSONObject();
        clusterJson.put("taskId", taskId);
        clusterJson.put("taskStatus", "DELETE");

        try {
            HttpClient.HttpResult result = HttpClient.httpPost(url, clusterJson.toJSONString());
            System.out.println(result.content);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
            JSONObject taskDetail = getTaskDetailByTaskId(taskId);
            Assert.assertEquals("DELETE", taskDetail.getString("taskStatus"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    @Test
    public void updateTaskWithStatusSync() {
        String url = baseUrl + "/v1/task/update";
        JSONObject clusterJson = new JSONObject();
        clusterJson.put("taskId", getTaskId());
        clusterJson.put("taskStatus", "SYNC");

        try {
            HttpClient.HttpResult result = HttpClient.httpPost(url, clusterJson.toJSONString());
            System.out.println(result.content);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    /**
     * update中必须包含taskStatus
     */
    @Test
    public void updateTaskWithServiceName() {
        String url = baseUrl + "/v1/task/update";
        JSONObject clusterJson = new JSONObject();
        clusterJson.put("taskId", getTaskId());
        String serviceName = "CI-Nacos-Service" + System.currentTimeMillis();
        clusterJson.put("serviceName", serviceName);

        try {
            HttpClient.HttpResult result = HttpClient.httpPost(url, clusterJson.toJSONString());
            System.out.println(result.content);
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertFalse(JSON.parseObject(result.content).getBoolean("success"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    private void addCluster(String clusterType) {
        String url = baseUrl + "/v1/cluster/add";

        JSONObject clusterJson = new JSONObject();
        clusterJson.put("clusterName", "CI-Nacos-Test" + System.currentTimeMillis());
        clusterJson.put("clusterType", clusterType);

        JSONArray jsonArray = new JSONArray();
        jsonArray.add("11.11.11.11");
        jsonArray.add("22.22.22.22");
        clusterJson.put("connectKeyList", jsonArray);

        try {
            HttpClient.HttpResult result = HttpClient.httpPost(url, clusterJson.toJSONString());
            Assert.assertEquals(HttpStatus.SC_OK, result.code);
            Assert.assertTrue(JSON.parseObject(result.content).getBoolean("success"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue("error", false);
        }
    }

    private String getClusterId(String clusterType) {
        String clusterId = "";
        String url = baseUrl + "/v1/cluster/list";
        try {
            Map<String, String> paramValues = new HashMap<>();
            paramValues.put("pageNum", "1");
            paramValues.put("pageSize", "50");

            HttpResult result = HttpClient.httpGet(url, null, paramValues);
            JSONArray clusterModels = JSON.parseObject(result.content).getJSONArray("clusterModels");
            if (clusterModels.size() > 0) {
                for (int i=0; i<clusterModels.size(); i++) {
                    if (clusterModels.getJSONObject(i).getString("clusterName").startsWith("CI-Nacos-Test") && clusterModels.getJSONObject(i).getString("clusterType").equals(clusterType)) {
                        clusterId = clusterModels.getJSONObject(i).getString("clusterId");
                        System.out.println("find cluster id = " + clusterId);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return clusterId;
    }

    private String getTaskId() {
        String taskId = "";
        String url = baseUrl + "/v1/task/list";
        try {
            Map<String, String> paramValues = new HashMap<>();
            paramValues.put("pageNum", "1");
            paramValues.put("pageSize", "10");

            HttpResult result = HttpClient.httpGet(url, null, paramValues);
            JSONArray taskModels = JSON.parseObject(result.content).getJSONArray("taskModels");

            if (taskModels.size() > 0) {
                for (int i=0; i<taskModels.size(); i++) {
                    if (taskModels.getJSONObject(i).getString("serviceName").startsWith("CI-Nacos-Service")) {
                        taskId = taskModels.getJSONObject(i).getString("taskId");
                        System.out.println("find task id = " + taskId);
                        break;
                    }
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return taskId;
    }

    private JSONObject getTaskDetailByTaskId(String taskId) {
        String url = baseUrl + "/v1/task/detail";
        Map<String, String> paramValues = new HashMap<>();
        paramValues.put("taskId", taskId);

        try {
            HttpClient.HttpResult result = HttpClient.httpGet(url, null, paramValues);
            if (HttpStatus.SC_OK == result.code) {
                return JSON.parseObject(result.content).getJSONObject("taskModel");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
        return null;
    }

}
