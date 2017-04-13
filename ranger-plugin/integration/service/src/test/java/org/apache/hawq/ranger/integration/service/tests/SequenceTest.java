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

import org.apache.hawq.ranger.integration.service.tests.common.ComplexResourceTestBase;
import org.apache.hawq.ranger.integration.service.tests.common.Policy;
import org.junit.Before;

import static org.apache.hawq.ranger.integration.service.tests.common.Policy.ResourceType.*;

public class SequenceTest extends ComplexResourceTestBase {

    @Before
    public void beforeTest() {
        specificResource.put(database, TEST_DB);
        specificResource.put(schema, TEST_SCHEMA);
        specificResource.put(sequence, TEST_SEQUENCE);

        parentUnknownResource.put(database, TEST_DB);
        parentUnknownResource.put(schema, UNKNOWN);
        parentUnknownResource.put(sequence, TEST_SEQUENCE);

        childUnknownResource.put(database, TEST_DB);
        childUnknownResource.put(schema, TEST_SCHEMA);
        childUnknownResource.put(sequence, UNKNOWN);

        unknownResource.put(database, UNKNOWN);
        unknownResource.put(schema, UNKNOWN);
        unknownResource.put(sequence, UNKNOWN);

        privileges = new String[] {"select", "update", "usage"};
    }

    @Override
    protected Policy getResourceUserPolicy() {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(schema, TEST_SCHEMA)
                .resource(sequence, TEST_SEQUENCE)
                .userAccess(TEST_USER, privileges)
                .build();
        return policy;
    }

    @Override

    protected Policy getResourceParentStarUserPolicy() {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(schema, STAR)
                .resource(sequence, TEST_SEQUENCE)
                .userAccess(TEST_USER, privileges)
                .build();
        policy.isParentStar = true;
        return policy;
    }

    @Override
    protected Policy getResourceChildStarUserPolicy() {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(schema, TEST_SCHEMA)
                .resource(sequence, STAR)
                .userAccess(TEST_USER, privileges)
                .build();
        policy.isChildStar = true;
        return policy;
    }

    @Override
    protected Policy getResourceGroupPolicy(String group) {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(schema, TEST_SCHEMA)
                .resource(sequence, TEST_SEQUENCE)
                .groupAccess(group, privileges)
                .build();
        return policy;
    }
}
