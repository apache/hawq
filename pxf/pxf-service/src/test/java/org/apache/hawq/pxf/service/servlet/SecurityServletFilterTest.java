package org.apache.hawq.pxf.service.servlet;

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
import org.apache.hawq.pxf.service.SessionId;
import org.apache.hawq.pxf.service.UGICache;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SecurityServletFilterTest {

    private static final String PROPERTY_KEY_USER_IMPERSONATION = "pxf.service.user.impersonation.enabled";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private HttpServletRequest servletRequest;

    @Before
    public void setup() {
        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "true");
        servletRequest = mock(HttpServletRequest.class);
    }

    @Test
    public void throwsWhenRequiredHeaderIsEmpty() throws Exception {

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Header X-GP-USER is empty in the request");

        when(servletRequest.getHeader("X-GP-USER")).thenReturn("  ");

        SecurityServletFilter securityServletFilter = new SecurityServletFilter();
        securityServletFilter.doFilter(servletRequest, mock(ServletResponse.class), mock(FilterChain.class));

    }

    @Test
    public void throwsWhenRequiredHeaderIsMissing() throws Exception {

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Header X-GP-USER is missing in the request");

        SecurityServletFilter securityServletFilter = new SecurityServletFilter();
        securityServletFilter.doFilter(servletRequest, mock(ServletResponse.class), mock(FilterChain.class));
    }

    @Test
    public void doesNotCleanTheUGICacheOnNonLastCalls() throws Exception {

        when(servletRequest.getHeader("X-GP-USER")).thenReturn("gpadmin");
        when(servletRequest.getHeader("X-GP-XID")).thenReturn("0");
        when(servletRequest.getHeader("X-GP-SEGMENT-ID")).thenReturn("1");

        SecurityServletFilter securityServletFilter = new SecurityServletFilter();
        securityServletFilter.init(mock(FilterConfig.class));
        securityServletFilter.proxyUGICache = mock(UGICache.class);

        when(securityServletFilter.proxyUGICache.getUserGroupInformation(any(SessionId.class))).thenReturn(mock(UserGroupInformation.class));

        securityServletFilter.doFilter(servletRequest, mock(ServletResponse.class), mock(FilterChain.class));

        verify(securityServletFilter.proxyUGICache).release(any(SessionId.class), eq(false));
    }

    @Test
    public void tellsTheUGICacheToCleanItselfOnTheLastCallForASegment() throws Exception {

        System.setProperty(PROPERTY_KEY_USER_IMPERSONATION, "true");
        HttpServletRequest servletRequest = mock(HttpServletRequest.class);

        when(servletRequest.getHeader("X-GP-USER")).thenReturn("gpadmin");
        when(servletRequest.getHeader("X-GP-XID")).thenReturn("0");
        when(servletRequest.getHeader("X-GP-SEGMENT-ID")).thenReturn("1");
        when(servletRequest.getHeader("X-GP-LAST-FRAGMENT")).thenReturn("true");

        SecurityServletFilter securityServletFilter = new SecurityServletFilter();
        securityServletFilter.init(mock(FilterConfig.class));
        securityServletFilter.proxyUGICache = mock(UGICache.class);

        when(securityServletFilter.proxyUGICache.getUserGroupInformation(any(SessionId.class))).thenReturn(mock(UserGroupInformation.class));

        securityServletFilter.doFilter(servletRequest, mock(ServletResponse.class), mock(FilterChain.class));

        verify(securityServletFilter.proxyUGICache).release(any(SessionId.class), eq(true));
    }
}
