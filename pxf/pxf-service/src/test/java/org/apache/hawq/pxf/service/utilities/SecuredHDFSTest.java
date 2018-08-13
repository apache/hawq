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


import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.servlet.ServletContext;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest({UserGroupInformation.class})
public class SecuredHDFSTest {
    ProtocolData mockProtocolData;
    ServletContext mockContext;

    @Test
    @SuppressWarnings("unchecked")
    public void nullToken() throws IOException {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(mockProtocolData.getToken()).thenReturn(null);
        when(UserGroupInformation.getLoginUser()).thenReturn(ugi);

        SecuredHDFS.verifyToken(null, mockContext);
        verify(ugi).reloginFromKeytab();
        verify(ugi, never()).addToken(any(Token.class));
    }

    @Test
    public void invalidTokenThrows() throws IOException {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(UserGroupInformation.getLoginUser()).thenReturn(ugi);
        when(mockProtocolData.getToken()).thenReturn("This is odd");

        try {
            SecuredHDFS.verifyToken("This is odd", mockContext);
            fail("invalid X-GP-TOKEN should throw");
        } catch (SecurityException e) {
            assertEquals("Failed to verify delegation token java.io.EOFException", e.getMessage());
        }
    }

    @Test
    public void loggedOutUser() throws IOException {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(UserGroupInformation.getLoginUser()).thenReturn(ugi);
        when(mockProtocolData.getToken()).thenReturn("This is odd");

        try {
            SecuredHDFS.verifyToken("This is odd", mockContext);
            fail("invalid X-GP-TOKEN should throw");
        } catch (SecurityException e) {
            verify(ugi).reloginFromKeytab();
            assertEquals("Failed to verify delegation token java.io.EOFException", e.getMessage());
        }
    }

    /*
     * setUp function called before each test
	 */
    @Before
    public void setUp() {
        mockProtocolData = mock(ProtocolData.class);
        mockContext = mock(ServletContext.class);

        PowerMockito.mockStatic(UserGroupInformation.class);
    }
}
