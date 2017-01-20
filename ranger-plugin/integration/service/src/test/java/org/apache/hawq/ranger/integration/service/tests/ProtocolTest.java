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

public class ProtocolTest extends ServiceTestBase {

    private static final List<String> PRIVILEGES = Arrays.asList("select", "insert");

    @Before
    public void beforeTest() throws IOException {
        createPolicy("test-protocol.json");
        resources.put("protocol", "pxf");
    }

    @Test
    public void testProtocols_UserMaria_PxfProtocol_Allowed() throws IOException {
        assertTrue(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testProtocols_UserMaria_DoesNotExistProtocol_Denied() throws IOException {
        resources.put("protocol", "doesnotexist");
        assertFalse(hasAccess(TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testProtocols_UserBob_PxfProtocol_Denied() throws IOException {
        assertFalse(hasAccess(UNKNOWN_USER, resources, PRIVILEGES));
    }

    @Test
    public void testProtocols_UserMaria_PxfProtocol_Denied() throws IOException {
        deletePolicy();
        assertFalse(hasAccess(TEST_USER, resources, PRIVILEGES));
    }
}
