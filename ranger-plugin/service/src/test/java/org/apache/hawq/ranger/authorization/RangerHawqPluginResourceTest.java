/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hawq.ranger.authorization;

import org.apache.hawq.ranger.authorization.model.AuthorizationRequest;
import org.apache.hawq.ranger.authorization.model.AuthorizationResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static junit.framework.TestCase.*;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RangerHawqAuthorizer.class)
public class RangerHawqPluginResourceTest {

    private RangerHawqPluginResource resource;

    @Mock
    private RangerHawqAuthorizer mockAuthorizer;
    @Mock
    private AuthorizationResponse mockResponse;
    @Mock
    private RuntimeException mockException;

    @Before
    public void setup() throws Exception {
        PowerMockito.mockStatic(RangerHawqAuthorizer.class);
        when(RangerHawqAuthorizer.getInstance()).thenReturn(mockAuthorizer);
        resource = new RangerHawqPluginResource();
    }

    @Test
    public void testGetVersion() {
        String version = (String) resource.version().getEntity();
        assertEquals("{\"version\":\"version-test\"}", version);
    }

    @Test
    public void testAuthorizeSuccess() {
        when(mockAuthorizer.isAccessAllowed(any(AuthorizationRequest.class))).thenReturn(mockResponse);
        AuthorizationResponse response = resource.authorize(new AuthorizationRequest());
        assertNotNull(response);
        assertEquals(mockResponse, response);
    }

    @Test
    public void testAuthorizeException() {
        when(mockAuthorizer.isAccessAllowed(any(AuthorizationRequest.class))).thenThrow(mockException);
        try {
            resource.authorize(new AuthorizationRequest());
            fail("should've thrown exception");
        } catch (Exception e) {
            assertSame(mockException, e);
        }
    }
}
