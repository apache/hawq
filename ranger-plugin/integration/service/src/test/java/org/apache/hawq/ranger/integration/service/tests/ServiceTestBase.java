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

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.ranger.integration.service.tests.policy.Policy;
import org.apache.hawq.ranger.integration.service.tests.policy.Policy.PolicyBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.*;

public abstract class ServiceTestBase {

    protected Log LOG = LogFactory.getLog(this.getClass());

    @Rule
    public final TestName testName = new TestName();

    protected static final String TEST_USER = "maria_dev";
    protected static final String UNKNOWN = "unknown";


    protected final String policyName = getClass().getSimpleName();
    protected Map<String, String> resources = new HashMap<>();
    protected PolicyBuilder policyBuilder;



    private static final String RPS_HOST = "localhost";
    private static final String RPS_PORT = "8432";
    private static final String RPS_URL = String.format("http://%s:%s/rps", RPS_HOST, RPS_PORT);

    private static final String RANGER_HOST = "localhost";
    private static final String RANGER_PORT = "6080";
    private static final String RANGER_URL = String.format("http://%s:%s/service/public/v2/api", RANGER_HOST, RANGER_PORT);
    private static final String RANGER_POLICY_URL = RANGER_URL + "/policy";

    private static int    POLICY_REFRESH_INTERVAL = 6000;

    private static final TypeReference<HashMap<String,Object>> typeMSO = new TypeReference<HashMap<String,Object>>() {};

    private RESTClient rest = new RESTClient();
    private ObjectMapper mapper = new ObjectMapper();


    @Before
    public void setUp() throws IOException {
        LOG.info("======================================================================================");
        LOG.info("Running test " + testName.getMethodName());
        LOG.info("======================================================================================");

        policyBuilder = (new PolicyBuilder()).name(policyName);
    }

    @After
    public void tearDown() throws IOException {
        deletePolicy();
    }

    protected void createPolicy(String jsonFile) throws IOException {
        LOG.info(String.format("Creating policy %s from file %s", policyName, jsonFile));
        rest.executeRequest(RESTClient.Method.POST, RANGER_POLICY_URL, readFile(jsonFile));
        waitForPolicyRefresh();
    }

    protected void createPolicy(Policy policy) throws IOException {
        LOG.info(String.format("Creating policy %s", policy.name));
        rest.executeRequest(RESTClient.Method.POST, RANGER_POLICY_URL, mapper.writeValueAsString(policy));
        waitForPolicyRefresh();
    }

    protected void deletePolicy() throws IOException {
        LOG.info("Deleting policy " + policyName);
        try {
            rest.executeRequest(RESTClient.Method.DELETE, getRangerPolicyUrl(policyName));
        } catch (RESTClient.ResourceNotFoundException e) {
            // ignore error when deleting a policy that does not exit
        }
        waitForPolicyRefresh();
    }

    protected boolean hasAccess(String user, Map<String, String> resources, String[] privileges) throws IOException {
        return hasAccess(user, resources, Arrays.asList(privileges));
    }

    protected boolean hasAccess(String user, Map<String, String> resources, List<String> privileges) throws IOException {
        LOG.info("Checking access for user " + user);
        String response = rest.executeRequest(RESTClient.Method.POST, RPS_URL, getRPSRequestPayload(user, resources, privileges));
        Map<String, Object> responseMap = mapper.readValue(response, typeMSO);
        boolean allowed = (Boolean)((Map)((List) responseMap.get("access")).get(0)).get("allowed");
        LOG.info(String.format("Access for user %s is allowed = %s", user, allowed));
        return allowed;
    }

    private void waitForPolicyRefresh() {
        try {
            Thread.sleep(POLICY_REFRESH_INTERVAL);
        }
        catch (InterruptedException e) {
            LOG.error(e);
        }
    }

    private String readFile(String fileName) throws IOException {
        return IOUtils.toString(ServiceTestBase.class.getClassLoader().getResourceAsStream(fileName));
    }

    private String getRangerPolicyUrl(String policyName) {
        return RANGER_POLICY_URL + "?servicename=hawq&policyname=" + policyName;
    }

    private String getRPSRequestPayload (String user, Map<String, String> resources, List<String> privileges) throws IOException {
        Map<String, Object> request = new HashMap<>();
        request.put("requestId", 9);
        request.put("user", user);
        request.put("clientIp", "123.0.0.21");
        request.put("context", "CREATE SOME DATABASE OBJECT;");

        Map<String, Object> access = new HashMap<>();
        access.put("resource", resources);
        access.put("privileges", privileges);

        Set<Map<String, Object>> accesses = new HashSet<>();
        accesses.add(access);
        request.put("access", accesses);
        return new ObjectMapper().writeValueAsString(request);
    }

}
