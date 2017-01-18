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

public class ListTablespacesTest extends LookupTestBase {

    private static final Set<String> TABLESPACES = Sets.newHashSet("pg_default", "pg_global", "dfs_default");

    @Test
    public void testListTablespace_All() throws Exception {
        List<String> result = service.lookupResource(getContext("tablespace", "*"));
        assertEquals(3, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet(TABLESPACES)));
    }

    @Test
    public void testListTablespace_FilteredPresent() throws Exception {
        List<String> result = service.lookupResource(getContext("tablespace", "pg_d"));
        assertEquals(1, result.size());
        assertTrue(Sets.newHashSet(result).equals(Sets.newHashSet("pg_default")));
    }

    @Test
    public void testListTablespace_FilteredAbsent() throws Exception {
        List<String> result = service.lookupResource(getContext("tablespace", "z"));
        assertTrue(result.isEmpty());
    }

}
