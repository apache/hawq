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
import static org.apache.hawq.ranger.integration.service.tests.policy.Policy.ResourceType.language;
import static org.junit.Assert.assertFalse;

public class LanguageTest extends ServiceTestBase {

    private static final String TEST_DB = "test-db";
    private static final String TEST_LANGUAGE = "test-language";

    @Before
    public void beforeTest() {
        specificResource.put(database, TEST_DB);
        specificResource.put(language, TEST_LANGUAGE);

        parentUnknownResource.put(database, UNKNOWN);
        parentUnknownResource.put(language, TEST_LANGUAGE);

        childUnknownResource.put(database, TEST_DB);
        childUnknownResource.put(language, UNKNOWN);

        unknownResource.put(database, UNKNOWN);
        unknownResource.put(language, UNKNOWN);

        privileges = new String[] {"usage"};
    }

    @Override
    protected Policy getResourceUserPolicy() {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(language, TEST_LANGUAGE)
                .userAccess(TEST_USER, privileges)
                .build();
        return policy;
    }

    protected Policy getResourceParentStarUserPolicy() {
        Policy policy = policyBuilder
                .resource(database, STAR)
                .resource(language, TEST_LANGUAGE)
                .userAccess(TEST_USER, privileges)
                .build();
        policy.isParentStar = true;
        return policy;
    }

    protected Policy getResourceChildStarUserPolicy() {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(language, STAR)
                .userAccess(TEST_USER, privileges)
                .build();
        policy.isChildStar = true;
        return policy;
    }

    @Override
    protected Policy getResourceGroupPolicy() {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(language, TEST_LANGUAGE)
                .groupAccess(PUBLIC_GROUP, privileges)
                .build();
        return policy;
    }

    @Test
    public void testParentStartResourceUserPolicy() throws IOException {
        checkResourceUserPolicy(getResourceParentStarUserPolicy());
    }

    @Test
    public void testChildStartResourceUserPolicy() throws IOException {
        checkResourceUserPolicy(getResourceChildStarUserPolicy());
    }
}
