package org.apache.hawq.pxf.service.utilities;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
public class SecureLoginTest {

    private String PROPERTY_KEY_USER_IMPERSONATION = "pxf.service.user.impersonation.enabled";

    @Test
    public void testImpersonationPropertyAbsent() {
        System.clearProperty(PROPERTY_KEY_USER_IMPERSONATION);
        assertFalse(SecureLogin.isUserImpersonationEnabled());
    }

    @Test
    public void testImpersonationPropertyEmpty() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "");
        assertFalse(SecureLogin.isUserImpersonationEnabled());
    }

    @Test
    public void testImpersonationPropertyFalse() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "foo");
        assertFalse(SecureLogin.isUserImpersonationEnabled());
    }

    @Test
    public void testImpersonationPropertyTRUE() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "TRUE");
        assertTrue(SecureLogin.isUserImpersonationEnabled());
    }

    @Test
    public void testImpersonationPropertyTrue() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "true");
        assertTrue(SecureLogin.isUserImpersonationEnabled());
    }

}

