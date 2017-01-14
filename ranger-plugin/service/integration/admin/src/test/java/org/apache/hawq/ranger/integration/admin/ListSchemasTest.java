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
 * specific schema governing permissions and limitations
 * under the License.
 */

package org.apache.hawq.ranger.integration.admin;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ListSchemasTest extends LookupTestBase {

    private static final Set<String> DEFAULT_SCHEMAS = Sets.newHashSet("public");
    private static final Set<String> EAST_SCHEMAS = Sets.newHashSet("common", "japan", "public");
    private static final Set<String> WEST_SCHEMAS = Sets.newHashSet("common", "france", "jamaica", "public");
    private static final Set<String> ALL_SCHEMAS = Sets.newHashSet("common", "japan", "france", "jamaica", "public");

    private Map<String, List<String>> resources;

    @Before
    public void setUp() {
        resources = new HashMap<>();
    }

    @Test
    public void testListSchema_NoResources() throws Exception {
        resources.put("database", Arrays.asList("noschema_db"));
        List<String> result = service.lookupResource(getContext("schema", "*", resources));
        assertEquals(DEFAULT_SCHEMAS.size(), result.size());
        assertEquals(DEFAULT_SCHEMAS, Sets.newHashSet(result));
    }

    @Test
    public void testListSchemas_SingleDb_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("east"));
        List<String> result = service.lookupResource(getContext("schema", "*", resources));
        assertEquals(EAST_SCHEMAS.size(), result.size());
        assertEquals(EAST_SCHEMAS, Sets.newHashSet(result));
    }

    @Test
    public void testListSchemas_TwoDb_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        List<String> result = service.lookupResource(getContext("schema", "*", resources));
        assertEquals(ALL_SCHEMAS.size(), result.size());
        assertEquals(ALL_SCHEMAS, Sets.newHashSet(result));
    }

    @Test
    public void testListSchemas_AllDb_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("schema", "*", resources));
        assertEquals(ALL_SCHEMAS.size(), result.size());
        assertEquals(ALL_SCHEMAS, Sets.newHashSet(result));
    }

    @Test
    public void testListSchemas_SingleDb_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("east"));
        List<String> result = service.lookupResource(getContext("schema", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSchemas_TwoDb_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        List<String> result = service.lookupResource(getContext("schema", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSchemas_AllDb_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("schema", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSchemas_SingleDb_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("east"));
        List<String> result = service.lookupResource(getContext("schema", "ja", resources));
        assertEquals(1, result.size());
        assertEquals("japan", result.get(0));
    }

    @Test
    public void testListSchemas_TwoDb_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        List<String> result = service.lookupResource(getContext("schema", "ja", resources));
        assertEquals(2, result.size());
        assertEquals(Sets.newHashSet("japan", "jamaica"), Sets.newHashSet(result));
    }

    @Test
    public void testListSchemas_AllDb_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("schema", "ja", resources));
        assertEquals(2, result.size());
        assertEquals(Sets.newHashSet("japan", "jamaica"), Sets.newHashSet(result));
    }

}