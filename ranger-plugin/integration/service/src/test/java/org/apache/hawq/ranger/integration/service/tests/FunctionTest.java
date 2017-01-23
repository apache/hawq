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

import org.apache.hawq.ranger.integration.service.tests.policy.Policy;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FunctionTest /*extends ServiceTestBase*/ {

    private static final List<String> PRIVILEGES = Arrays.asList("execute");

    /*
    @Override
    protected Policy getResourceUserPolicy() {
        return null;
    }

    @Override
    protected Policy getResourceGroupPolicy() {
        return null;
    }


    @Before
    public void beforeTest() throws IOException {
        createPolicy("test-function.json");
        resources.put("database", "sirotan");
        resources.put("schema", "siroschema");
        resources.put("function", "atan");
    }

    @Test
    public void testFunctions_UserMaria_SirotanDb_AtanFunction_Allowed() throws IOException {
        assertTrue(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testFunctions_UserMaria_OtherDb_AtanFunction_Denied() throws IOException {
        resources.put("database", "other");
        assertFalse(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testFunctions_UserMaria_SirotanDb_DoesNotExistFunction_Denied() throws IOException {
        resources.put("function", "doesnotexist");
        assertFalse(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testFunctions_UserBob_SirotanDb_AtanFunction_Denied() throws IOException {
        assertFalse(hasAccess(UNKNOWN, resources, PRIVILEGES));
    }

    @Test
    public void testFunctions_UserMaria_SirotanDb_AtanFunction_Denied() throws IOException {
        deletePolicy();
        assertFalse(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testFunctions_UserMaria_DoesNotExistDb_AtanFunction_Denied() throws IOException {
        resources.put("database", "doesnotexist");
        assertFalse(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testFunctions_UserMaria_SirotanDb_AtanFunction_Policy2_Allowed() throws IOException {
        deletePolicy();
        createPolicy("test-function-2.json");
        assertTrue(hasAccess(TEST_USER, resources, PRIVILEGES));
    }
    */
}
