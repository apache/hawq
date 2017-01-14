/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hawq.ranger.integration.service.tests;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ServiceBaseTest {

    protected static final Log log = LogFactory.getLog(ServiceBaseTest.class);
    protected HttpClient httpClient = HttpClientBuilder.create().build();
    protected ObjectMapper mapper = new ObjectMapper();

    public static String RANGER_PLUGIN_SERVICE_HOST = "localhost";
    public static String RANGER_PLUGIN_SERVICE_PORT = "8432";
    public static String RANGER_PLUGIN_SERVICE_URL =
        "http://" + RANGER_PLUGIN_SERVICE_HOST + ":" + RANGER_PLUGIN_SERVICE_PORT + "/rps";
    public static String RANGER_ADMIN_HOST = "localhost";
    public static String RANGER_ADMIN_PORT = "6080";
    public static String RANGER_URL =
        "http://" + RANGER_ADMIN_HOST + ":" + RANGER_ADMIN_PORT + "/service/public/v2/api";
    public static String RANGER_TEST_USER = "maria_dev";

    protected void setHttpHeaders(HttpRequestBase request) throws IOException {
        request.setHeader("Authorization", "Basic " + getEncoding());
        request.setHeader("Content-Type", "application/json");
    }

    protected void processHttpRequest(HttpRequestBase request) throws IOException {
        setHttpHeaders(request);
        log.info("Request URI = " + request.getURI().toString());
        HttpResponse response = httpClient.execute(request);
        int responseCode = response.getStatusLine().getStatusCode();
        log.info("Response Code = " + responseCode);
        if (log.isDebugEnabled()) {
            HttpEntity entity = response.getEntity();
            if (entity == null) {
               log.debug("The response has no entity");
            }
            else {
               log.debug(IOUtils.toString(response.getEntity().getContent()));
            }
        }
    }

    protected void createPolicy(String jsonFile) throws IOException {
        HttpPost httpPost = new HttpPost(RANGER_URL + "/policy");
        httpPost.setEntity(new StringEntity(getPayload(jsonFile)));
        processHttpRequest(httpPost);
    }

    protected void deletePolicy(String policyName) throws IOException {
        HttpDelete httpDelete = new HttpDelete(RANGER_URL + "/policy?servicename=hawq&policyname=" + policyName);
        processHttpRequest(httpDelete);
    }

    protected boolean hasAccess(String user, Map<String, String> resources, List<String> privileges) throws IOException {
        HttpPost httpPost = new HttpPost(RANGER_PLUGIN_SERVICE_URL);
        httpPost.setHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(mapper.writeValueAsString(createJsonRequest(user, resources, privileges))));
        HttpResponse response = httpClient.execute(httpPost);
        String result = IOUtils.toString(response.getEntity().getContent());
        RPSResponse rpsResponse = new ObjectMapper().readValue(result, RPSResponse.class);
        return (Boolean) rpsResponse.getAccess().get(0).get("allowed");
    }

    private Map<String, Object> createJsonRequest(String user, Map<String, String> resources, List<String> privileges) {
        Map<String, Object> request = new HashMap<>();
        request.put("requestId", 9);
        request.put("user", user);
        request.put("clientIp", "123.0.0.21");
        request.put("context", "CREATE DATABASE sirotan;");
        Map<String, Object> accessHash = new HashMap<>();
        accessHash.put("resource", resources);
        accessHash.put("privileges", privileges);
        request.put("access", Arrays.asList(accessHash));
        return request;
    }

    private String getPayload(String jsonFile) throws IOException {
        return IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream(jsonFile));
    }

    private String getEncoding() {
        return Base64.encodeBase64String("admin:admin".getBytes());
    }

}
