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

package org.apache.hawq.ranger.integration.admin;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ListLanguagesTest extends LookupTestBase {

    private static final Set<String> DEFAULT_LANGUAGES = Sets.newHashSet("c", "internal", "plpgsql", "sql");
    private static final Set<String> EAST_LANGUAGES = Sets.newHashSet("langdbeast", "c", "internal", "plpgsql", "sql");

    private Map<String, List<String>> resources = new HashMap<>();

    @Test
    public void testListLanguage_NoResources() throws Exception {
        resources.put("database", Arrays.asList("noschema_db"));
        List<String> result = service.lookupResource(getContext("language", "*", resources));
        assertEquals(4, result.size());
        assertTrue(Sets.newHashSet(result).equals(DEFAULT_LANGUAGES));
    }

    @Test
    public void testListLanguages_SingleDb_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("east"));
        List<String> result = service.lookupResource(getContext("language", "*", resources));
        assertEquals(5, result.size());
        assertTrue(Sets.newHashSet(result).equals(EAST_LANGUAGES));
    }

    @Test
    public void testListLanguages_TwoDb_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        List<String> result = service.lookupResource(getContext("language", "*", resources));
        assertEquals(6, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("langdbeast", "langdbwest", "c", "internal", "plpgsql", "sql")));
    }

    @Test
    public void testListLanguages_AllDb_AllFilter() throws Exception {
        resources.put("database", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("language", "*", resources));
        assertEquals(6, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("langdbeast", "langdbwest", "c", "internal", "plpgsql", "sql")));
    }

    @Test
    public void testListLanguages_SingleDb_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("east"));
        List<String> result = service.lookupResource(getContext("language", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListLanguages_TwoDb_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        List<String> result = service.lookupResource(getContext("language", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListLanguages_AllDb_FilteredAbsent() throws Exception {
        resources.put("database", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("language", "z", resources));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListLanguages_SingleDb_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("east"));
        List<String> result = service.lookupResource(getContext("language", "l", resources));
        assertEquals(1, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("langdbeast")));
    }

    @Test
    public void testListLanguages_TwoDb_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("east", "west"));
        List<String> result = service.lookupResource(getContext("language", "l", resources));
        assertEquals(2, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("langdbeast", "langdbwest")));
    }

    @Test
    public void testListLanguages_AllDb_FilteredPresent() throws Exception {
        resources.put("database", Arrays.asList("*"));
        List<String> result = service.lookupResource(getContext("language", "l", resources));
        assertEquals(2, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("langdbeast", "langdbwest")));
    }

}
