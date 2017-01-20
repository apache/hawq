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

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DatabaseTest extends ServiceTestBase {

    private static final List<String> PRIVILEGES = Arrays.asList("connect", "temp");

    @Before
    public void beforeTest() throws IOException {
        createPolicy("test-database.json");
        resources.put("database", "sirotan");
    }

    @Test
    public void testDatabases_UserMaria_SirotanDb_Allowed() throws IOException {
        assertTrue(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testDatabases_UserMaria_DoesNotExistDb_Denied() throws IOException {
        resources.put("database", "doesnotexist");
        assertFalse(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testDatabases_UserBob_SirotanDb_Denied() throws IOException {
        assertFalse(hasAccess(UNKNOWN_USER, resources, PRIVILEGES));
    }

    @Test
    public void testDatabases_UserMaria_SirotanDb_Denied() throws IOException {
        deletePolicy();
        assertFalse(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

}
