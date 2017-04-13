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

import static org.apache.hawq.ranger.integration.service.tests.common.Policy.ResourceType.database;
import static org.apache.hawq.ranger.integration.service.tests.common.Policy.ResourceType.language;

public class LanguageTest extends ComplexResourceTestBase {

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

    @Override
    protected Policy getResourceParentStarUserPolicy() {
        Policy policy = policyBuilder
                .resource(database, STAR)
                .resource(language, TEST_LANGUAGE)
                .userAccess(TEST_USER, privileges)
                .build();
        policy.isParentStar = true;
        return policy;
    }

    @Override
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
    protected Policy getResourceGroupPolicy(String group) {
        Policy policy = policyBuilder
                .resource(database, TEST_DB)
                .resource(language, TEST_LANGUAGE)
                .groupAccess(group, privileges)
                .build();
        return policy;
    }
}
