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

import static org.apache.hawq.ranger.integration.service.tests.policy.Policy.ResourceType.database;
import static org.apache.hawq.ranger.integration.service.tests.policy.Policy.ResourceType.schema;
import static org.apache.hawq.ranger.integration.service.tests.policy.Policy.ResourceType.table;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DatabaseTest extends ServiceTestBase {

    private static final String[] PRIVILEGES = {"connect", "temp"};
    private static final String TEST_DB = "test-user-db";

    @Before
    public void beforeTest() throws IOException {
        Policy policy = policyBuilder.resource(database, TEST_DB).userAccess(TEST_USER, PRIVILEGES).build();
        createPolicy(policy);

        specificResource.put(database, TEST_DB);
        unknownResource.put(database, UNKNOWN);
    }

    @Test
    public void testSpecificResource_UserPolicy() throws IOException {

        // create user policy
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(schema, STAR)
                .resource(table, STAR)
                .userAccess(TEST_USER, PRIVILEGES)
                .build();
        createPolicy(policy);

        // user IN the policy --> has all possible privileges to the specific resource
        assertTrue(hasAccess(TEST_USER, specificResource, PRIVILEGES));
        for (String privilege : PRIVILEGES) {
            // user IN the policy --> has individual privileges to the specific resource
            assertTrue(hasAccess(TEST_USER, specificResource, privilege));
        }

        // user NOT in the policy --> has NO access to the specific resource
        assertFalse(hasAccess(UNKNOWN, specificResource, PRIVILEGES));

        // user IN the policy --> has NO access to the unknown resource
        assertFalse(hasAccess(TEST_USER, unknownResource, PRIVILEGES));

        // delete the policy
        deletePolicy();
        assertFalse(hasAccess(TEST_USER, specificResource, PRIVILEGES));

    }

    /*
    @Test
    public void testDatabase_Explicit_User_Allowed() throws IOException {
        assertTrue(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testDatabase_Unknown_User_Denied() throws IOException {
        resources.put("database", UNKNOWN);
        assertFalse(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testDatabase_UserBob_SirotanDb_Denied() throws IOException {
        assertFalse(hasAccess(UNKNOWN, resources, PRIVILEGES));
    }

    @Test
    public void testDatabase_UserMaria_SirotanDb_Denied() throws IOException {
        deletePolicy();
        assertFalse(hasAccess(TEST_USER, resources, PRIVILEGES));
    }
    */

}
