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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ListDatabasesTest extends LookupTestBase {

    private static final Set<String> DATABASES = Sets.newHashSet("postgres", "east", "west", "noschema_db");

    @Test
    public void testListDatabases_All() throws Exception {
        List<String> result = service.lookupResource(getContext("database", "*"));
        assertEquals(4, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet(DATABASES)));
    }

    @Test
    public void testListDatabases_FilteredPresent() throws Exception {
        List<String> result = service.lookupResource(getContext("database", "e"));
        assertEquals(1, result.size());
        assertEquals(result.get(0), "east");
    }

    @Test
    public void testListDatabases_FilteredAbsent() throws Exception {
        List<String> result = service.lookupResource(getContext("database", "z"));
        assertTrue(result.isEmpty());
    }

}
