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

import org.apache.hawq.ranger.integration.service.tests.common.Policy;
import org.apache.hawq.ranger.integration.service.tests.common.ServiceTestBase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hawq.ranger.integration.service.tests.common.Policy.ResourceType.*;

public class SpecialPrivilegesTest extends ServiceTestBase {

    private final String[] privilegeUsageSchema = new String[] {"usage-schema"};
    private final String[] privilegeUsage = new String[] {"usage"};


    private Map<Policy.ResourceType, String> schemaResource;
    private Map<Policy.ResourceType, String> sequenceResource;

    @Before
    public void beforeTest() {
        // resource used for lookup from RPS
        schemaResource = new HashMap<>();
        schemaResource.put(database, TEST_DB);
        schemaResource.put(schema, TEST_SCHEMA);

        sequenceResource = new HashMap<>();
        sequenceResource.put(database, TEST_DB);
        sequenceResource.put(schema, TEST_SCHEMA);
        sequenceResource.put(sequence, TEST_SEQUENCE);
    }


    @Test
    public void testUsageSchemaPrivilege() throws IOException {
        // define policy for "usage-schema" on db:schema:*
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(schema, TEST_SCHEMA)
                .resource(sequence, STAR)
                .userAccess(TEST_USER, privilegeUsageSchema)
                .build();
        createPolicy(policy);
        try {
            // user should have access to usage on schema
            checkUserHasResourceAccess(TEST_USER, schemaResource, privilegeUsage);
            // user should have NO access to usage on sequence
            checkUserDeniedResourceAccess(TEST_USER, sequenceResource, privilegeUsage);
        } finally {
            deletePolicy(policy);
        }
    }

    @Test
    public void testUsagePrivilege() throws IOException {
        // define policy for "usage" on db:schema:*
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(schema, TEST_SCHEMA)
                .resource(sequence, STAR)
                .userAccess(TEST_USER, privilegeUsage)
                .build();
        createPolicy(policy);
        try {
            // user should have NO access to usage on schema
            checkUserDeniedResourceAccess(TEST_USER, schemaResource, privilegeUsage);
            // user should have access to usage on sequence
            checkUserHasResourceAccess(TEST_USER, sequenceResource, privilegeUsage);
        } finally {
            deletePolicy(policy);
        }
    }
}
