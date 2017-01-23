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
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class ServiceTestBase {

    protected Log LOG = LogFactory.getLog(this.getClass());

    @Rule
    public final TestName testName = new TestName();

    protected static final String PUBLIC_GROUP = "public";
    protected static final String GPADMIN_USER = "gpadmin";
    protected static final String TEST_USER = "maria_dev";
    protected static final String UNKNOWN = "unknown";
    protected static final String STAR = "*";

    protected Map<String, String> resources = new HashMap<>();
    protected String[] privileges = {};
    protected Map<Policy.ResourceType, String> specificResource = new HashMap<>();
    protected Map<Policy.ResourceType, String> parentUnknownResource = new HashMap<>();
    protected Map<Policy.ResourceType, String> childUnknownResource = new HashMap<>();
    protected Map<Policy.ResourceType, String> unknownResource = new HashMap<>();
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

        policyBuilder = (new PolicyBuilder()).name(getClass().getSimpleName());
        specificResource = new HashMap<>();
        unknownResource = new HashMap<>();
    }

    protected void createPolicy(Policy policy) throws IOException {
        LOG.info(String.format("Creating policy %s", policy.name));
        rest.executeRequest(RESTClient.Method.POST, RANGER_POLICY_URL, mapper.writeValueAsString(policy));
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
        String response = rest.executeRequest(RESTClient.Method.POST, RPS_URL, getRPSRequestPayloadNew(user, resources, privileges));
        Map<String, Object> responseMap = mapper.readValue(response, typeMSO);
        boolean allowed = (Boolean)((Map)((List) responseMap.get("access")).get(0)).get("allowed");
        LOG.info(String.format("Access for user %s is allowed = %s", user, allowed));
        return allowed;
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

    private String getRPSRequestPayloadNew (String user, Map<Policy.ResourceType, String> resources, String[] privileges) throws IOException {
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


    protected void checkSpecificResource(String user) throws IOException {
        // user IN the policy --> has all possible privileges to the specific resource
        assertTrue(hasAccess(user, specificResource, privileges));
        for (String privilege : privileges) {
            // user IN the policy --> has individual privileges to the specific resource
            assertTrue(hasAccess(user, specificResource, privilege));
        }
    }

    protected void checkResourceUserPolicy(Policy policy) throws IOException {
        createPolicy(policy);
        checkSpecificResource(TEST_USER);
        // user NOT in the policy --> has NO access to the specific resource
        assertFalse(hasAccess(UNKNOWN, specificResource, privileges));

        // if resource has parents, assert edge cases
        if (!parentUnknownResource.isEmpty()) {
            // user IN the policy --> has access to the resource only for parentStar policies
            assertEquals(policy.isParentStar, hasAccess(TEST_USER, parentUnknownResource, privileges));
        }
        if (!childUnknownResource.isEmpty()) {
            // user IN the policy --> has access to the resource only for childStar policies
            assertEquals(policy.isChildStar, hasAccess(TEST_USER, childUnknownResource, privileges));
        }

        // user IN the policy --> has NO access to the unknown resource
        assertFalse(hasAccess(TEST_USER, unknownResource, privileges));
        // test that user doesn't have access if policy is deleted
        deletePolicy(policy);
        assertFalse(hasAccess(TEST_USER, specificResource, privileges));
    }

    abstract protected Policy getResourceUserPolicy();
    abstract protected Policy getResourceGroupPolicy();

    @Test
    public void testSpecificResourceUserPolicy() throws IOException {
        checkResourceUserPolicy(getResourceUserPolicy());
    }

    @Test
    public void testStarResourceGpadminPolicy() throws IOException {
        checkSpecificResource(GPADMIN_USER);
        // user NOT in the policy --> has NO access to the specific resource
        assertFalse(hasAccess(UNKNOWN, specificResource, privileges));
        // test that other existing user can't rely on gpadmin policy
        assertFalse(hasAccess(TEST_USER, specificResource, privileges));
        // user IN the policy --> has access to the unknown resource
        assertTrue(hasAccess(GPADMIN_USER, unknownResource, privileges));
    }

    @Test
    public void testSpecificResourcePublicGroupPolicy() throws IOException {
        Policy policy = getResourceGroupPolicy();
        createPolicy(policy);
        checkSpecificResource(TEST_USER);
        // user NOT in the policy --> has access to the specific resource
        assertTrue(hasAccess(UNKNOWN, specificResource, privileges));
        // user IN the policy --> has NO access to the unknown resource
        assertFalse(hasAccess(TEST_USER, unknownResource, privileges));
        // test that user doesn't have access if policy is deleted
        deletePolicy(policy);
        assertFalse(hasAccess(TEST_USER, specificResource, privileges));
    }

}
