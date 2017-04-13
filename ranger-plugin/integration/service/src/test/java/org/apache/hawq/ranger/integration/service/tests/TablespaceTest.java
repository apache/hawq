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
import org.apache.hawq.ranger.integration.service.tests.common.SimpleResourceTestBase;
import org.junit.Before;

import static org.apache.hawq.ranger.integration.service.tests.common.Policy.ResourceType.tablespace;

public class TablespaceTest extends SimpleResourceTestBase {

    @Before
    public void beforeTest() {
        specificResource.put(tablespace, TEST_TABLESPACE);
        unknownResource.put(tablespace, UNKNOWN);
        privileges = new String[] {"create"};
    }

    @Override
    protected Policy getResourceUserPolicy() {
        Policy policy = policyBuilder
                .resource(tablespace, TEST_TABLESPACE)
                .userAccess(TEST_USER, privileges)
                .build();
        return policy;
    }

    @Override
    protected Policy getResourceGroupPolicy(String group) {
        Policy policy = policyBuilder
                .resource(tablespace, TEST_TABLESPACE)
                .groupAccess(group, privileges)
                .build();
        return policy;
    }
}
