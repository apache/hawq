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

import org.apache.hawq.ranger.integration.service.tests.policy.Policy;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hawq.ranger.integration.service.tests.policy.Policy.ResourceType.database;
import static org.apache.hawq.ranger.integration.service.tests.policy.Policy.ResourceType.schema;
import static org.apache.hawq.ranger.integration.service.tests.policy.Policy.ResourceType.table;

public class SchemaTest extends ServiceTestBase {

    private static final String TEST_DB = "test-db";
    private static final String TEST_SCHEMA = "test-schema";

    // for schema only, privileges in policy must have -schema suffix added, create-schema is covered as part of DatabaseTest
    private static final String[] SPECIAL_PRIVILEGES = new String[] {"usage-schema"};

    @Before
    public void beforeTest() {
        specificResource.put(database, TEST_DB);
        specificResource.put(schema, TEST_SCHEMA);

        parentUnknownResource.put(database, UNKNOWN);
        parentUnknownResource.put(schema, TEST_SCHEMA);

        childUnknownResource.put(database, TEST_DB);
        childUnknownResource.put(schema, UNKNOWN);

        unknownResource.put(database, UNKNOWN);
        unknownResource.put(schema, UNKNOWN);

        privileges = new String[] {"usage"};
    }

    @Override
    protected Policy getResourceUserPolicy() {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(schema, TEST_SCHEMA)
                .resource(table, STAR)
                .userAccess(TEST_USER, SPECIAL_PRIVILEGES)
                .build();
        return policy;
    }

    protected Policy getResourceParentStarUserPolicy() {
        Policy policy = policyBuilder
                .resource(database, STAR)
                .resource(schema, TEST_SCHEMA)
                .resource(table, STAR)
                .userAccess(TEST_USER, SPECIAL_PRIVILEGES)
                .build();
        policy.isParentStar = true;
        return policy;
    }

    protected Policy getResourceChildStarUserPolicy() {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(schema, STAR)
                .resource(table, STAR)
                .userAccess(TEST_USER, SPECIAL_PRIVILEGES)
                .build();
        policy.isChildStar = true;
        return policy;
    }

    @Override
    protected Policy getResourceGroupPolicy() {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(schema, TEST_SCHEMA)
                .resource(table, STAR)
                .groupAccess(PUBLIC_GROUP, SPECIAL_PRIVILEGES)
                .build();
        return policy;
    }

    @Test
    public void testParentStarResourceUserPolicy() throws IOException {
        checkResourceUserPolicy(getResourceParentStarUserPolicy());
    }

    @Test
    public void testChildStarResourceUserPolicy() throws IOException {
        checkResourceUserPolicy(getResourceChildStarUserPolicy());
    }
}
