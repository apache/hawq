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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ServiceBaseTest {

    protected final Log log = LogFactory.getLog(this.getClass());

    @Rule
    public final TestName testName = new TestName();
    protected final String policyName = getClass().getSimpleName();
    protected Map<String, String> resources = new HashMap<>();

    public static String RANGER_PLUGIN_SERVICE_HOST = "localhost";
    public static String RANGER_PLUGIN_SERVICE_PORT = "8432";
    public static String RANGER_PLUGIN_SERVICE_URL =
        "http://" + RANGER_PLUGIN_SERVICE_HOST + ":" + RANGER_PLUGIN_SERVICE_PORT + "/rps";
    public static String RANGER_ADMIN_HOST = "localhost";
    public static String RANGER_ADMIN_PORT = "6080";
    public static String RANGER_URL =
        "http://" + RANGER_ADMIN_HOST + ":" + RANGER_ADMIN_PORT + "/service/public/v2/api";
    public static String RANGER_TEST_USER = "maria_dev";
    public static int    POLICY_REFRESH_INTERVAL = 6000;

    @Before
    public void setUp()
            throws IOException {
        log.info("======================================================================================");
        log.info("Running test " + testName.getMethodName());
        log.info("======================================================================================");
        beforeTest();
    }

    @After
    public void tearDown()
            throws IOException {
        deletePolicy();
    }

    protected void createPolicy(String jsonFile)
            throws IOException {

        log.info("Creating policy " + policyName);
        HttpPost httpPost = new HttpPost(RANGER_URL + "/policy");
        httpPost.setEntity(new StringEntity(Utils.getPayload(jsonFile)));
        Utils.processHttpRequest(httpPost);
        waitForPolicyRefresh();
    }

    protected void deletePolicy()
            throws IOException {

        log.info("Deleting policy " + policyName);
        String requestUrl = RANGER_URL + "/policy?servicename=hawq&policyname=" + policyName;
        Utils.processHttpRequest(new HttpDelete(requestUrl));
        waitForPolicyRefresh();
    }

    protected boolean hasAccess(String user,
                                Map<String, String> resources,
                                List<String> privileges)
            throws IOException {

        log.info("Checking access for user " + user);
        RPSRequest request = new RPSRequest(user, resources, privileges);
        HttpPost httpPost = new HttpPost(RANGER_PLUGIN_SERVICE_URL);
        httpPost.setEntity(new StringEntity(request.getJsonString()));
        String result = Utils.processHttpRequest(httpPost);
        RPSResponse rpsResponse = Utils.getResponse(result);
        return rpsResponse.hasAccess();
    }

    private void waitForPolicyRefresh() {

        try {
            Thread.sleep(POLICY_REFRESH_INTERVAL);
        }
        catch (InterruptedException e) {
            log.error(e);
        }
    }

    public abstract void beforeTest() throws IOException;
}
