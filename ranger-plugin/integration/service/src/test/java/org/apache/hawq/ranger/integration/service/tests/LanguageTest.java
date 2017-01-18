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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LanguageTest extends ServiceBaseTest {

    private static final List<String> PRIVILEGES = Arrays.asList("usage");
    private Map<String, String> resources = new HashMap<>();

    @Before
    public void setUp() throws IOException {
        createPolicy("test-language.json");
        resources.put("database", "sirotan");
        resources.put("language", "sql");
    }

    @After
    public void tearDown() throws IOException {
        deletePolicy(getClass().getSimpleName());
    }

    @Test
    public void testLanguages_UserMaria_SirotanDb_SqlLanguage_Allowed() throws Exception {
        assertTrue(hasAccess(RANGER_TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testLanguages_UserMaria_SirotanDb_DoesNotExistLanguage_Denied() throws Exception {
        resources.put("language", "doesnotexist");
        assertFalse(hasAccess(RANGER_TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testLanguages_UserBob_SirotanDb_SqlLanguage_Denied() throws Exception {
        assertFalse(hasAccess("bob", resources, PRIVILEGES));
    }

    @Test
    public void testLanguages_UserMaria_SirotanDb_SqlLanguage_Denied() throws IOException {
        deletePolicy(getClass().getSimpleName());
        assertFalse(hasAccess(RANGER_TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testLanguages_UserMaria_DoesNotExistDb_SqlLanguage_Denied() throws Exception {
        resources.put("database", "doesnotexist");
        assertFalse(hasAccess(RANGER_TEST_USER, resources, PRIVILEGES));
    }

    @Test
    public void testLanguages_UserMaria_SirotanDb_SqlLanguage_Policy2_Allowed() throws IOException {
        deletePolicy(getClass().getSimpleName());
        createPolicy("test-language-2.json");
        assertTrue(hasAccess(RANGER_TEST_USER, resources, PRIVILEGES));
    }

}
