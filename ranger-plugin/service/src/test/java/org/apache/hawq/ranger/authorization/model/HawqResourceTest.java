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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class HawqResourceTest {

    @Test
    public void testCaseInsensitiveDeserialization() throws IOException {
        assertNull(HawqResource.fromString(null));
        assertSame(HawqResource.database, HawqResource.fromString("database"));
        assertSame(HawqResource.database, HawqResource.fromString("DATABASE"));
        assertSame(HawqResource.database, HawqResource.fromString("datABAse"));

        ObjectMapper mapper = new ObjectMapper();
        assertSame(HawqResource.database, mapper.readValue("{\"value\": \"database\"}", ResourceHolder.class).value);
        assertSame(HawqResource.database, mapper.readValue("{\"value\": \"DATABASE\"}", ResourceHolder.class).value);
        assertSame(HawqResource.database, mapper.readValue("{\"value\": \"datABAse\"}", ResourceHolder.class).value);
    }

    public static class ResourceHolder {
        public HawqResource value;
    }
}
