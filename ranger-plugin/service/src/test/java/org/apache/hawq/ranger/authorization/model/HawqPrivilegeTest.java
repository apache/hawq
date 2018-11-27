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

package org.apache.hawq.ranger.authorization.model;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class HawqPrivilegeTest {

    @Test
    public void testSerialization() throws IOException {
        assertEquals("create", HawqPrivilege.create.toValue());
        assertEquals("create-schema", HawqPrivilege.create_schema.toValue());
        assertEquals("usage-schema", HawqPrivilege.usage_schema.toValue());

        ObjectMapper mapper = new ObjectMapper();
        assertEquals("{\"value\":\"create\"}", mapper.writeValueAsString(new PrivilegeHolder(HawqPrivilege.create)));
        assertEquals("{\"value\":\"create-schema\"}", mapper.writeValueAsString(new PrivilegeHolder(HawqPrivilege.create_schema)));
        assertEquals("{\"value\":\"usage-schema\"}", mapper.writeValueAsString(new PrivilegeHolder(HawqPrivilege.usage_schema)));
    }

    @Test
    public void testDeserialization() throws IOException {
        assertNull(HawqPrivilege.fromString(null));
        assertSame(HawqPrivilege.create, HawqPrivilege.fromString("create"));
        assertSame(HawqPrivilege.create, HawqPrivilege.fromString("CREATE"));
        assertSame(HawqPrivilege.create, HawqPrivilege.fromString("CreATe"));
        assertSame(HawqPrivilege.create_schema, HawqPrivilege.fromString("CreATe-schema"));
        assertSame(HawqPrivilege.usage_schema, HawqPrivilege.fromString("USage-schema"));


        ObjectMapper mapper = new ObjectMapper();
        assertSame(HawqPrivilege.create, mapper.readValue("{\"value\": \"create\"}", PrivilegeHolder.class).value);
        assertSame(HawqPrivilege.create, mapper.readValue("{\"value\": \"CREATE\"}", PrivilegeHolder.class).value);
        assertSame(HawqPrivilege.create, mapper.readValue("{\"value\": \"creATe\"}", PrivilegeHolder.class).value);
        assertSame(HawqPrivilege.create_schema, mapper.readValue("{\"value\": \"CreATe-schema\"}", PrivilegeHolder.class).value);
        assertSame(HawqPrivilege.usage_schema, mapper.readValue("{\"value\": \"USage-schema\"}", PrivilegeHolder.class).value);
    }

    public static class PrivilegeHolder {
        public HawqPrivilege value;
        PrivilegeHolder () {
        }
        PrivilegeHolder(HawqPrivilege value) {
            this.value = value;
        }
    }
}
