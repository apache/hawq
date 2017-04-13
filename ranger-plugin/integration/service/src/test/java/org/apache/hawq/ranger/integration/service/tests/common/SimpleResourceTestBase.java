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

package org.apache.hawq.ranger.integration.service.tests.common;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public abstract class SimpleResourceTestBase extends ServiceTestBase {

    protected Map<Policy.ResourceType, String> specificResource = new HashMap<>();
    protected Map<Policy.ResourceType, String> parentUnknownResource = new HashMap<>();
    protected Map<Policy.ResourceType, String> childUnknownResource = new HashMap<>();
    protected Map<Policy.ResourceType, String> unknownResource = new HashMap<>();
    protected String[] privileges = {};

    @Before
    public void beforeSimple() throws IOException {
        specificResource = new HashMap<>();
        parentUnknownResource = new HashMap<>();
        childUnknownResource = new HashMap<>();
        unknownResource = new HashMap<>();
        privileges = new String[]{};
    }

    @Test
    public void testSpecificResourceUserPolicy() throws IOException {
        checkResourceUserPolicy(getResourceUserPolicy());
    }

    @Test
    public void testStarResourceGpadminPolicy() throws IOException {
        checkUserHasResourceAccess(GPADMIN_USER, specificResource, privileges);
        // user NOT in the policy --> has NO access to the specific resource
        assertFalse(hasAccess(UNKNOWN, specificResource, privileges));
        // test that other existing user can't rely on gpadmin policy
        assertFalse(hasAccess(TEST_USER, specificResource, privileges));
        // user IN the policy --> has access to the unknown resource
        assertTrue(hasAccess(GPADMIN_USER, unknownResource, privileges));
    }

    @Test
    public void testSpecificResourcePublicGroupPolicy() throws IOException {
        Policy policy = getResourceGroupPolicy(PUBLIC_GROUP);
        createPolicy(policy);
        checkUserHasResourceAccess(TEST_USER, specificResource, privileges);
        // user NOT in the policy --> has access to the specific resource
        assertTrue(hasAccess(UNKNOWN, specificResource, privileges));
        // user IN the policy --> has NO access to the unknown resource
        assertFalse(hasAccess(TEST_USER, unknownResource, privileges));
        // test that user doesn't have access if policy is deleted
        deletePolicy(policy);
        assertFalse(hasAccess(TEST_USER, specificResource, privileges));
    }

    @Test
    public void testSpecificResourceUserGroupPolicy() throws IOException {
        Policy policy = getResourceGroupPolicy(TEST_GROUP);
        createPolicy(policy);
        checkUserHasResourceAccess(TEST_USER, specificResource, privileges);
        // user NOT in the group --> has NO access to the specific resource
        assertFalse(hasAccess(UNKNOWN, specificResource, privileges));
        // user IN the group --> has NO access to the unknown resource
        assertFalse(hasAccess(TEST_USER, unknownResource, privileges));
        // test that user doesn't have access if policy is deleted
        deletePolicy(policy);
        assertFalse(hasAccess(TEST_USER, specificResource, privileges));
    }

    protected void checkResourceUserPolicy(Policy policy) throws IOException {
        createPolicy(policy);
        boolean policyDeleted = false;
        try {
            checkUserHasResourceAccess(TEST_USER, specificResource, privileges);
            // user NOT in the policy --> has NO access to the specific resource
            LOG.debug(String.format("Asserting user %s NO  access %s privileges %s", UNKNOWN, specificResource, Arrays.toString(privileges)));
            assertFalse(hasAccess(UNKNOWN, specificResource, privileges));

            // if resource has parents, assert edge cases
            if (!parentUnknownResource.isEmpty()) {
                // user IN the policy --> has access to the resource only for parentStar policies
                assertEquals(policy.isParentStar, hasAccess(TEST_USER, parentUnknownResource, privileges));
            }
            if (!childUnknownResource.isEmpty()) {
                // user IN the policy --> has access to the resource only for childStar policies
                assertEquals(policy.isChildStar, hasAccess(TEST_USER, childUnknownResource, privileges));
            }

            // user IN the policy --> has NO access to the unknown resource
            assertFalse(hasAccess(TEST_USER, unknownResource, privileges));
            // test that user doesn't have access if policy is deleted
            deletePolicy(policy);
            policyDeleted = true;
            assertFalse(hasAccess(TEST_USER, specificResource, privileges));
        } finally {
            // if a given test fails with assertion, still delete the policy not to impact other tests
            if (!policyDeleted) {
                deletePolicy(policy);
            }
        }
    }

    abstract protected Policy getResourceUserPolicy();
    abstract protected Policy getResourceGroupPolicy(String group);
}
