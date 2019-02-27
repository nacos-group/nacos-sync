/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacossync.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.FluentStringsMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/**
 * @author nacos
 */
public class HttpClient {
    public static final int TIME_OUT_MILLIS = 10000;
    public static final int CON_TIME_OUT_MILLIS = 5000;

    private static AsyncHttpClient asyncHttpClient;

    private static CloseableHttpClient putClient;

    private static CloseableHttpClient postClient;

    static {
        AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
        builder.setMaximumConnectionsTotal(-1);
        builder.setMaximumConnectionsPerHost(-1);
        builder.setAllowPoolingConnection(true);
        builder.setFollowRedirects(false);
        builder.setIdleConnectionTimeoutInMs(TIME_OUT_MILLIS);
        builder.setConnectionTimeoutInMs(CON_TIME_OUT_MILLIS);
        builder.setCompressionEnabled(true);
        builder.setIOThreadMultiplier(1);
        builder.setMaxRequestRetry(0);
        builder.setUserAgent("");

        asyncHttpClient = new AsyncHttpClient(builder.build());

        HttpClientBuilder builder2 = HttpClients.custom();

        builder2.setUserAgent("");
        builder2.setConnectionTimeToLive(CON_TIME_OUT_MILLIS, TimeUnit.MILLISECONDS);
        builder2.setMaxConnTotal(-1);
        builder2.setMaxConnPerRoute(256);
        builder2.disableAutomaticRetries();

        putClient = builder2.build();
    }

    public static HttpResult httpGet(String url, List<String> headers, Map<String, String> paramValues) {
        return httpGetWithTimeOut(url, headers, paramValues, CON_TIME_OUT_MILLIS, TIME_OUT_MILLIS, "UTF-8");
    }

    public static HttpResult httpGetWithTimeOut(String url, List<String> headers, Map<String, String> paramValues, int connectTimeout, int readTimeout) {

        return httpGetWithTimeOut(url, headers, paramValues, connectTimeout, readTimeout, "UTF-8");
    }

    public static HttpResult httpGetWithTimeOut(String url, List<String> headers, Map<String, String> paramValues, int connectTimeout, int readTimeout, String encoding) {
        HttpURLConnection conn = null;
        try {
            String encodedContent = encodingParams(paramValues, encoding);
            url += (null == encodedContent) ? "" : ("?" + encodedContent);

            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(connectTimeout);
            conn.setReadTimeout(readTimeout);
            conn.setRequestMethod("GET");

            conn.addRequestProperty("Client-Version", "");
            setHeaders(conn, headers, encoding);
            conn.connect();

            return getResult(conn);
        } catch (Exception e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }


    public static HttpResult httpPut2(String url, List<String> headers, Map<String, String> paramValues, String encoding) {

        long start = System.currentTimeMillis();
        HttpURLConnection conn = null;
        try {
            String encodedContent = encodingParams(paramValues, encoding);
            url += (null == encodedContent) ? "" : ("?" + encodedContent);

            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(CON_TIME_OUT_MILLIS);
            conn.setReadTimeout(TIME_OUT_MILLIS);
            conn.setRequestMethod("PUT");
            setHeaders(conn, headers, encoding);
            conn.connect();

//            System.out.println("http connect: " + (System.currentTimeMillis() - start));

            HttpResult result = getResult(conn);

//            System.out.println("http response: " + (System.currentTimeMillis() - start));

            return result;

        } catch (Exception e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public static HttpResult httpGet(String url, List<String> headers, Map<String, String> paramValues, String encoding) {

        long start = System.currentTimeMillis();
        HttpURLConnection conn = null;
        try {
            String encodedContent = encodingParams(paramValues, encoding);
            url += (null == encodedContent) ? "" : ("?" + encodedContent);

            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(CON_TIME_OUT_MILLIS);
            conn.setReadTimeout(TIME_OUT_MILLIS);
            conn.setRequestMethod("GET");
            setHeaders(conn, headers, encoding);
            conn.connect();

//            System.out.println("http connect: " + (System.currentTimeMillis() - start));

            HttpResult result = getResult(conn);

//            System.out.println("http response: " + (System.currentTimeMillis() - start));

            return result;

        } catch (Exception e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public static void asyncHttpGet(String url, List<String> headers, Map<String, String> paramValues, AsyncCompletionHandler handler) throws Exception {
        if (!MapUtils.isEmpty(paramValues)) {
            String encodedContent = encodingParams(paramValues, "UTF-8");
            url += (null == encodedContent) ? "" : ("?" + encodedContent);
        }

        AsyncHttpClient.BoundRequestBuilder builder = asyncHttpClient.prepareGet(url);

        if (!CollectionUtils.isEmpty(headers)) {
            for (String header : headers) {
                builder.setHeader(header.split("=")[0], header.split("=")[1]);
            }
        }

        builder.setHeader("Accept-Charset", "UTF-8");

        if (handler != null) {
            builder.execute(handler);
        } else {
            builder.execute();
        }
    }

    public static void asyncHttpPostLarge(String url, List<String> headers, String content, AsyncCompletionHandler handler) throws Exception {
        AsyncHttpClient.BoundRequestBuilder builder = asyncHttpClient.preparePost(url);

        if (!CollectionUtils.isEmpty(headers)) {
            for (String header : headers) {
                builder.setHeader(header.split("=")[0], header.split("=")[1]);
            }
        }

        builder.setBody(content.getBytes("UTF-8"));

        builder.setHeader("Content-Type", "application/json; charset=UTF-8");
        builder.setHeader("Accept-Charset", "UTF-8");
        builder.setHeader("Accept-Encoding", "gzip");
        builder.setHeader("Content-Encoding", "gzip");

        if (handler != null) {
            builder.execute(handler);
        } else {
            builder.execute();
        }
    }

    public static void asyncHttpPostLarge(String url, List<String> headers, byte[] content, AsyncCompletionHandler handler) throws Exception {
        AsyncHttpClient.BoundRequestBuilder builder = asyncHttpClient.preparePost(url);

        if (!CollectionUtils.isEmpty(headers)) {
            for (String header : headers) {
                builder.setHeader(header.split("=")[0], header.split("=")[1]);
            }
        }

        builder.setBody(content);

        builder.setHeader("Content-Type", "application/json; charset=UTF-8");
        builder.setHeader("Accept-Charset", "UTF-8");
        builder.setHeader("Accept-Encoding", "gzip");
        builder.setHeader("Content-Encoding", "gzip");

        if (handler != null) {
            builder.execute(handler);
        } else {
            builder.execute();
        }
    }

    public static void asyncHttpPost(String url, List<String> headers, Map<String, String> paramValues, AsyncCompletionHandler handler) throws Exception {
        AsyncHttpClient.BoundRequestBuilder builder = asyncHttpClient.preparePost(url);

        if (!CollectionUtils.isEmpty(headers)) {
            for (String header : headers) {
                builder.setHeader(header.split("=")[0], header.split("=")[1]);
            }
        }

        if (!MapUtils.isEmpty(paramValues)) {
            FluentStringsMap params = new FluentStringsMap();
            for (Map.Entry<String, String> entry : paramValues.entrySet()) {
                params.put(entry.getKey(), Collections.singletonList(entry.getValue()));
            }

            builder.setParameters(params);
        }

        builder.setHeader("Accept-Charset", "UTF-8");

        if (handler != null) {
            builder.execute(handler);
        } else {
            builder.execute();
        }
    }

    public static HttpResult httpPost(String url, List<String> headers, Map<String, String> paramValues) {
        return httpPost(url, headers, paramValues, "UTF-8");
    }

    public static HttpResult httpPost(String url, String bodyJson) {

        try {

            HttpPost httpost = new HttpPost(url);

            RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(5000).setConnectTimeout(5000).setSocketTimeout(5000).setRedirectsEnabled(true).setMaxRedirects(5).build();
            httpost.setConfig(requestConfig);

            StringEntity s = new StringEntity(bodyJson);
            s.setContentEncoding("UTF-8");
            s.setContentType("application/json");

            httpost.setEntity(s);
            HttpResponse response = putClient.execute(httpost);
            HttpEntity entity = response.getEntity();

            String charset = "UTF-8";
            if (entity != null && entity.getContentType() != null) {

                HeaderElement[] headerElements = entity.getContentType().getElements();

                if (headerElements != null && headerElements.length > 0 && headerElements[0] != null &&
                        headerElements[0].getParameterByName("charset") != null) {
                    charset = headerElements[0].getParameterByName("charset").getValue();
                }
            }

            return new HttpResult(response.getStatusLine().getStatusCode(),
                    entity == null ? "" : IOUtils.toString(entity.getContent(), charset), Collections.<String, String>emptyMap());
        } catch (Throwable e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        }
    }

    public static HttpResult httpPost(String url, Map<String, String> headers, String bodyJson) {

        try {

            HttpPost httpost = new HttpPost(url);

            RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(5000).setConnectTimeout(5000).setSocketTimeout(5000).setRedirectsEnabled(true).setMaxRedirects(5).build();
            httpost.setConfig(requestConfig);

            StringEntity s = new StringEntity(bodyJson);
            s.setContentEncoding("UTF-8");
            s.setContentType("application/json");

            httpost.setEntity(s);
            if (!headers.isEmpty()) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    httpost.setHeader(entry.getKey(), entry.getValue());
                }
            }
            HttpResponse response = putClient.execute(httpost);
            HttpEntity entity = response.getEntity();

            String charset = "UTF-8";
            if (entity != null && entity.getContentType() != null) {

                HeaderElement[] headerElements = entity.getContentType().getElements();

                if (headerElements != null && headerElements.length > 0 && headerElements[0] != null &&
                    headerElements[0].getParameterByName("charset") != null) {
                    charset = headerElements[0].getParameterByName("charset").getValue();
                }
            }

            return new HttpResult(response.getStatusLine().getStatusCode(),
                entity == null ? "" : IOUtils.toString(entity.getContent(), charset), Collections.<String, String>emptyMap());
        } catch (Throwable e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        }
    }


    public static HttpResult httpPost(String url, List<String> headers, Map<String, String> paramValues, String encoding) {
        try {
            HttpClientBuilder builder = HttpClients.custom();
            builder.setUserAgent("");
            builder.setConnectionTimeToLive(CON_TIME_OUT_MILLIS, TimeUnit.MILLISECONDS);

            CloseableHttpClient httpClient = builder.build();
            HttpPost httpost = new HttpPost(url);

            RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(5000).setConnectTimeout(5000).setSocketTimeout(5000).setRedirectsEnabled(true).setMaxRedirects(5).build();
            httpost.setConfig(requestConfig);

            List<NameValuePair> nvps = new ArrayList<NameValuePair>();

            for (Map.Entry<String, String> entry : paramValues.entrySet()) {
                nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }


            httpost.setEntity(new UrlEncodedFormEntity(nvps, encoding));
            HttpResponse response = httpClient.execute(httpost);
            HttpEntity entity = response.getEntity();

            String charset = encoding;
            if (entity.getContentType() != null) {

                HeaderElement[] headerElements = entity.getContentType().getElements();

                if (headerElements != null && headerElements.length > 0 && headerElements[0] != null &&
                        headerElements[0].getParameterByName("charset") != null) {
                    charset = headerElements[0].getParameterByName("charset").getValue();
                }
            }

            return new HttpResult(response.getStatusLine().getStatusCode(), IOUtils
                .toString(entity.getContent(), charset), Collections.<String, String>emptyMap());
        } catch (Throwable e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        }
    }

    public static HttpResult httpPost(String url, Map<String, String> headers, Map<String, String> paramValues, String encoding) {
        try {
            HttpClientBuilder builder = HttpClients.custom();
            builder.setUserAgent("");
            builder.setConnectionTimeToLive(CON_TIME_OUT_MILLIS, TimeUnit.MILLISECONDS);

            CloseableHttpClient httpClient = builder.build();
            HttpPost httpost = new HttpPost(url);

            RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(5000).setConnectTimeout(5000).setSocketTimeout(5000).setRedirectsEnabled(true).setMaxRedirects(5).build();
            httpost.setConfig(requestConfig);
            if (!headers.isEmpty()) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    httpost.setHeader(entry.getKey(), entry.getValue());
                }
            }

            List<NameValuePair> nvps = new ArrayList<NameValuePair>();

            for (Map.Entry<String, String> entry : paramValues.entrySet()) {
                nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }

            httpost.setEntity(new UrlEncodedFormEntity(nvps, encoding));
            HttpResponse response = httpClient.execute(httpost);
            HttpEntity entity = response.getEntity();

            String charset = encoding;
            if (entity.getContentType() != null) {

                HeaderElement[] headerElements = entity.getContentType().getElements();

                if (headerElements != null && headerElements.length > 0 && headerElements[0] != null &&
                    headerElements[0].getParameterByName("charset") != null) {
                    charset = headerElements[0].getParameterByName("charset").getValue();
                }
            }

            return new HttpResult(response.getStatusLine().getStatusCode(), IOUtils
                .toString(entity.getContent(), charset), Collections.<String, String>emptyMap());
        } catch (Throwable e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        }
    }


    public static HttpResult httpPut(String url, List<String> headers, Map<String, String> paramValues, String encoding) {
        try {

            HttpPut httput = new HttpPut(url);

            RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(5000).setConnectTimeout(5000).setSocketTimeout(5000).setRedirectsEnabled(true).setMaxRedirects(5).build();
            httput.setConfig(requestConfig);

            List<NameValuePair> nvps = new ArrayList<NameValuePair>();

            for (Map.Entry<String, String> entry : paramValues.entrySet()) {
                nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }

            httput.setEntity(new UrlEncodedFormEntity(nvps, encoding));
            HttpResponse response = putClient.execute(httput);
            HttpEntity entity = response.getEntity();

            String charset = encoding;
            if (entity.getContentType() != null) {

                HeaderElement[] headerElements = entity.getContentType().getElements();

                if (headerElements != null && headerElements.length > 0 && headerElements[0] != null &&
                        headerElements[0].getParameterByName("charset") != null) {
                    charset = headerElements[0].getParameterByName("charset").getValue();
                }
            }

            return new HttpResult(response.getStatusLine().getStatusCode(), IOUtils
                .toString(entity.getContent(), charset), Collections.<String, String>emptyMap());
        } catch (Throwable e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        }
    }

    public static HttpResult httpDelete(String url, String someParam, Map<String, String> headerMap, String encoding) {
        try {
            String jsonStr = null;
            if (someParam != null) {
                url += "/" + someParam;
            }
            HttpDelete httpDelete = new HttpDelete(url);
            if (null != headerMap && !headerMap.isEmpty()) {
                for (String key : headerMap.keySet()) {
                    httpDelete.setHeader(key, headerMap.get(key));
                }
            }

            CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            HttpResponse response = httpClient.execute(httpDelete);
            HttpEntity entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                jsonStr = EntityUtils.toString(entity, "UTF-8");
                httpClient.close();
            } else {
                jsonStr = EntityUtils.toString(entity, "UTF-8");
                httpClient.close();
                throw new IllegalArgumentException(jsonStr);
            }
            return new HttpResult(response.getStatusLine().getStatusCode(), IOUtils
                .toString(entity.getContent(), encoding), Collections.<String, String>emptyMap());
        } catch (Throwable e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        }
    }

    public static HttpResult httpDelete(String url, Map<String, String> paramValues, Map<String, String> headerMap, String encoding) {
        CloseableHttpClient httpClient = null;
        try {
            String jsonStr = null;
            if (!MapUtils.isEmpty(paramValues)) {
                String encodedContent = encodingParams(paramValues, "UTF-8");
                url += (null == encodedContent) ? "" : ("?" + encodedContent);
            }

            HttpDelete httpDelete = new HttpDelete(url);
            if (null != headerMap && !headerMap.isEmpty()) {
                for (String key : headerMap.keySet()) {
                    httpDelete.setHeader(key, headerMap.get(key));
                }
            }

            httpClient = HttpClientBuilder.create().build();
            HttpResponse response = httpClient.execute(httpDelete);
            HttpEntity entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                jsonStr = EntityUtils.toString(entity, "UTF-8");
                //httpClient.close();
            } else {
                jsonStr = EntityUtils.toString(entity, "UTF-8");
                //httpClient.close();
                throw new IllegalArgumentException(jsonStr);
            }
            return new HttpResult(HttpStatus.SC_OK, jsonStr, Collections.<String, String>emptyMap());
        } catch (Throwable e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        } finally {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static HttpResult httpPostLarge(String url, Map<String, String> headers, String content) {
        try {
            HttpClientBuilder builder = HttpClients.custom();
            builder.setUserAgent("");
            builder.setConnectionTimeToLive(500, TimeUnit.MILLISECONDS);

            CloseableHttpClient httpClient = builder.build();
            HttpPost httpost = new HttpPost(url);

            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpost.setHeader(entry.getKey(), entry.getValue());
            }

            httpost.setEntity(new StringEntity(content, ContentType.create("application/json", "UTF-8")));
            HttpResponse response = httpClient.execute(httpost);
            HttpEntity entity = response.getEntity();

            HeaderElement[] headerElements = entity.getContentType().getElements();
            String charset = headerElements[0].getParameterByName("charset").getValue();

            return new HttpResult(response.getStatusLine().getStatusCode(),
                    IOUtils.toString(entity.getContent(), charset), Collections.<String, String>emptyMap());
        } catch (Exception e) {
            return new HttpResult(500, e.toString(), Collections.<String, String>emptyMap());
        }
    }

    private static HttpResult getResult(HttpURLConnection conn) throws IOException {
        int respCode = conn.getResponseCode();

        InputStream inputStream;
        if (HttpURLConnection.HTTP_OK == respCode) {
            inputStream = conn.getInputStream();
        } else {
            inputStream = conn.getErrorStream();
        }

        Map<String, String> respHeaders = new HashMap<String, String>(conn.getHeaderFields().size());
        for (Map.Entry<String, List<String>> entry : conn.getHeaderFields().entrySet()) {
            respHeaders.put(entry.getKey(), entry.getValue().get(0));
        }

        String gzipEncoding = "gzip";

        if (gzipEncoding.equals(respHeaders.get(HttpHeaders.CONTENT_ENCODING))) {
            inputStream = new GZIPInputStream(inputStream);
        }

        HttpResult result = new HttpResult(respCode, IOUtils.toString(inputStream, getCharset(conn)), respHeaders);
        inputStream.close();

        return result;
    }

    private static String getCharset(HttpURLConnection conn) {
        String contentType = conn.getContentType();
        if (StringUtils.isEmpty(contentType)) {
            return "UTF-8";
        }

        String[] values = contentType.split(";");
        if (values.length == 0) {
            return "UTF-8";
        }

        String charset = "UTF-8";
        for (String value : values) {
            value = value.trim();

            if (value.toLowerCase().startsWith("charset=")) {
                charset = value.substring("charset=".length());
            }
        }

        return charset;
    }

    private static void setHeaders(HttpURLConnection conn, List<String> headers, String encoding) {
        if (null != headers) {
            for (Iterator<String> iter = headers.iterator(); iter.hasNext(); ) {
                conn.addRequestProperty(iter.next(), iter.next());
            }
        }

        conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset="
                + encoding);
        //conn.addRequestProperty("Accept-Charset", encoding);
        //conn.addRequestProperty("Client-Version", "");
    }

    public static String encodingParams(Map<String, String> params, String encoding)
            throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        if (null == params || params.isEmpty()) {
            return null;
        }

        params.put("encoding", encoding);
        params.put("nofix", "1");

        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (StringUtils.isEmpty(entry.getValue())) {
                continue;
            }

            sb.append(entry.getKey()).append("=");
            sb.append(URLEncoder.encode(entry.getValue(), encoding));
            sb.append("&");
        }

        return sb.toString();
    }

    public static class HttpResult {
        final public int code;
        final public String content;
        final private Map<String, String> respHeaders;

        public HttpResult(int code, String content, Map<String, String> respHeaders) {
            this.code = code;
            this.content = content;
            this.respHeaders = respHeaders;
        }

        public String getHeader(String name) {
            return respHeaders.get(name);
        }
    }
}
