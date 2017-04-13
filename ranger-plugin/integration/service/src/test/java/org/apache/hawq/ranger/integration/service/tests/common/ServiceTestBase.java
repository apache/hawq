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

package org.apache.hawq.ranger.integration.service.tests.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.ranger.integration.service.tests.common.Policy.PolicyBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class ServiceTestBase {

    protected Log LOG = LogFactory.getLog(this.getClass());

    @Rule
    public final TestName testName = new TestName();

    protected static final String PUBLIC_GROUP = "public";
    protected static final String TEST_GROUP = "test";
    protected static final String GPADMIN_USER = "gpadmin";
    protected static final String TEST_USER = "maria_dev";
    protected static final String UNKNOWN = "unknown";
    protected static final String STAR = "*";

    protected static final String TEST_DB = "test-db";
    protected static final String TEST_SCHEMA = "test-schema";
    protected static final String TEST_TABLE = "test-table";
    protected static final String TEST_FUNCTION = "test-function";
    protected static final String TEST_SEQUENCE = "test-sequence";
    protected static final String TEST_LANGUAGE = "test-language";
    protected static final String TEST_PROTOCOL = "test-protocol";
    protected static final String TEST_TABLESPACE = "test-tablespace";

    protected PolicyBuilder policyBuilder;

    private static final String RPS_HOST = "localhost";
    private static final String RPS_PORT = "8432";
    private static final String RPS_URL = String.format("http://%s:%s/rps", RPS_HOST, RPS_PORT);

    private static final String RANGER_HOST = "localhost";
    private static final String RANGER_PORT = "6080";
    private static final String RANGER_URL = String.format("http://%s:%s/service/public/v2/api", RANGER_HOST, RANGER_PORT);
    private static final String RANGER_POLICY_URL = RANGER_URL + "/policy";

    private static final String POLICY_WAIT_INTERVAL_PROP_NAME = "policy.wait.interval.ms";
    private static final int POLICY_WAIT_INTERVAL = Integer.parseInt(System.getProperty(POLICY_WAIT_INTERVAL_PROP_NAME, "6000"));
    private static final TypeReference<HashMap<String,Object>> typeMSO = new TypeReference<HashMap<String,Object>>() {};

    private RESTClient rest = new RESTClient();
    private ObjectMapper mapper = new ObjectMapper();

    @Before
    public void setUp() throws IOException {
        LOG.info("======================================================================================");
        LOG.info("Running test " + testName.getMethodName());
        LOG.info("======================================================================================");

        policyBuilder = (new PolicyBuilder()).name(getClass().getSimpleName());
    }

    protected void checkUserHasResourceAccess(String user, Map<Policy.ResourceType, String> resource, String[] privileges) throws IOException {
        // user IN the policy --> has all possible privileges to the specific resource
        LOG.debug(String.format("Asserting user %s HAS access %s privileges %s", user, resource, Arrays.toString(privileges)));
        assertTrue(hasAccess(user, resource, privileges));
        for (String privilege : privileges) {
            // user IN the policy --> has individual privileges to the specific resource
            LOG.debug(String.format("Asserting user %s HAS access %s privilege %s", user, resource, privilege));
            assertTrue(hasAccess(user, resource, privilege));
        }
    }

    protected void checkUserDeniedResourceAccess(String user, Map<Policy.ResourceType, String> resource, String[] privileges) throws IOException {
        // user IN the policy --> has all possible privileges to the specific resource
        LOG.debug(String.format("Asserting user %s HAS NO access %s privileges %s", user, resource, Arrays.toString(privileges)));
        assertFalse(hasAccess(user, resource, privileges));
        for (String privilege : privileges) {
            // user IN the policy --> has individual privileges to the specific resource
            LOG.debug(String.format("Asserting user %s HAS No access %s privilege %s", user, resource, privilege));
            assertFalse(hasAccess(user, resource, privilege));
        }
    }

    protected void createPolicy(Policy policy) throws IOException {
        String policyJson = mapper.writeValueAsString(policy);
        LOG.info(String.format("Creating policy %s : %s", policy.name, policyJson));
        rest.executeRequest(RESTClient.Method.POST, RANGER_POLICY_URL, policyJson);
        waitForPolicyRefresh();
    }

    protected void deletePolicy(Policy policy) throws IOException {
        LOG.info("Deleting policy " + policy.name);
        try {
            rest.executeRequest(RESTClient.Method.DELETE, getRangerPolicyUrl(policy.name));
        } catch (RESTClient.ResourceNotFoundException e) {
            // ignore error when deleting a policy that does not exit
        }
        waitForPolicyRefresh();
    }

    protected boolean hasAccess(String user, Map<Policy.ResourceType, String> resources, String... privileges) throws IOException {
        LOG.info("Checking access for user " + user);
        String response = rest.executeRequest(RESTClient.Method.POST, RPS_URL, getRPSRequestPayload(user, resources, privileges));
        Map<String, Object> responseMap = mapper.readValue(response, typeMSO);
        boolean allowed = (Boolean)((Map)((List) responseMap.get("access")).get(0)).get("allowed");
        LOG.info(String.format("Access for user %s is allowed = %s", user, allowed));
        return allowed;
    }

    private void waitForPolicyRefresh() {
        try {
            Thread.sleep(POLICY_WAIT_INTERVAL);
        }
        catch (InterruptedException e) {
            LOG.error(e);
        }
    }

    private String getRangerPolicyUrl(String policyName) {
        return RANGER_POLICY_URL + "?servicename=hawq&policyname=" + policyName;
    }

    private String getRPSRequestPayload(String user, Map<Policy.ResourceType, String> resources, String[] privileges) throws IOException {
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
