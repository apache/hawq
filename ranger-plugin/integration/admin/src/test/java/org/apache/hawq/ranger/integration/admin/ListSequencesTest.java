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
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ListSequencesTest extends LookupTestBase {

    private Map<String, List<String>> resources;

    @Before
    public void setUp() {
        resources = new HashMap<>();
    }

    @Test
    public void testListSequences_NoSchemaDb_AllSchemas_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("noschema_db"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "*", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSequences_SingleDb_SingleSchema_AllFilter_NoSequences() throws Exception {
        resources.put("database", Arrays.asList("west"));
        resources.put("schema", Arrays.asList("jamaica"));
        List<String> result = service.lookupResource(getContext("sequence", "*", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSequences_SingleDb_SingleSchema_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("east"));
        resources.put("schema", Arrays.asList("japan"));
        List<String> result = service.lookupResource(getContext("sequence", "*", resources));
        assertEquals(2, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("water", "sake")));
    }

    @Test
    public void testListSequences_SingleDb_TwoSchemas_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("east"));
        resources.put("schema", Arrays.asList("common", "japan"));
        List<String> result = service.lookupResource(getContext("sequence", "*", resources));
        assertEquals(3, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("water", "sprite", "sake")));
    }

    @Test
    public void testListSequences_SingleDb_AllSchemas_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("east"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "*", resources));
        assertEquals(3, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("water", "sprite", "sake")));
    }

    @Test
    public void testListSequences_TwoDb_CommonSchema_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        resources.put("schema", Arrays.asList("common"));
        List<String> result = service.lookupResource(getContext("sequence", "*", resources));
        assertEquals(2, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("water", "sprite")));
    }

    @Test
    public void testListSequences_TwoDb_SingleSchema_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        resources.put("schema", Arrays.asList("japan"));
        List<String> result = service.lookupResource(getContext("sequence", "*", resources));
        assertEquals(2, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("water", "sake")));
    }

    @Test
    public void testListSequences_TwoDb_AllSchemas_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "*", resources));
        assertEquals(4, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("water", "sprite", "sake", "scotch")));
    }

    @Test
    public void testListSequences_AllDb_AllSchemas_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("*"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "*", resources));
        assertEquals(4, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("water", "sprite", "sake", "scotch")));
    }

    @Test
    public void testListSequences_SingleDb_SingleSchema_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("east"));
        resources.put("schema", Arrays.asList("japan"));
        List<String> result = service.lookupResource(getContext("sequence", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSequences_SingleDb_TwoSchemas_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("east"));
        resources.put("schema", Arrays.asList("common", "japan"));
        List<String> result = service.lookupResource(getContext("sequence", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSequences_SingleDb_AllSchemas_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("east"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSequences_TwoDbs_CommonSchema_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        resources.put("schema", Arrays.asList("common"));
        List<String> result = service.lookupResource(getContext("sequence", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSequences_TwoDbs_SingleSchema_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        resources.put("schema", Arrays.asList("japan"));
        List<String> result = service.lookupResource(getContext("sequence", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSequences_TwoDbs_AllSchemas_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSequences_AllDbs_AllSchemas_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("*"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListSequences_SingleDb_SingleSchema_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("east"));
        resources.put("schema", Arrays.asList("japan"));
        List<String> result = service.lookupResource(getContext("sequence", "s", resources));
        assertEquals(1, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("sake")));
    }

    @Test
    public void testListSequences_SingleDb_TwoSchemas_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("east"));
        resources.put("schema", Arrays.asList("common", "japan"));
        List<String> result = service.lookupResource(getContext("sequence", "s", resources));
        assertEquals(2, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("sprite", "sake")));
    }

    @Test
    public void testListSequences_SingleDb_AllSchemas_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("east"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "s", resources));
        assertEquals(2, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("sprite", "sake")));
    }

    @Test
    public void testListSequences_SingleDb_AllSchemas_FilteredPresent2() throws Exception {
        resources.put("database", Arrays.asList("east"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "w", resources));
        assertEquals(1, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("water")));
    }

    @Test
    public void testListSequences_TwoDbs_CommonSchema_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        resources.put("schema", Arrays.asList("common"));
        List<String> result = service.lookupResource(getContext("sequence", "w", resources));
        assertEquals(1, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("water")));
    }

    @Test
    public void testListSequences_TwoDbs_SingleSchema_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        resources.put("schema", Arrays.asList("japan"));
        List<String> result = service.lookupResource(getContext("sequence", "s", resources));
        assertEquals(1, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("sake")));
    }

    @Test
    public void testListSequences_TwoDbs_AllSchemas_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "s", resources));
        assertEquals(3, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("sprite", "sake", "scotch")));
    }

    @Test
    public void testListSequences_AllDbs_AllSchemas_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("*"));
        resources.put("schema", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("sequence", "s", resources));
        assertEquals(3, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("sprite", "sake", "scotch")));
    }

}