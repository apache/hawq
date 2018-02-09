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


import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hawq.pxf.service.utilities.SecureLogin;


/**
 * Listener on lifecycle events of our webapp
 */
public class SecurityServletFilter implements Filter {

    private static final Log LOG = LogFactory.getLog(SecurityServletFilter.class);
    private static final String USER_HEADER = "X-GP-USER";
    private static final String MISSING_HEADER_ERROR = String.format("Header %s is missing in the request", USER_HEADER);
    private static final String EMPTY_HEADER_ERROR = String.format("Header %s is empty in the request", USER_HEADER);

    /**
     * Initializes the filter.
     *
     * @param filterConfig filter configuration
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    /**
     * If user impersonation is configured, examines the request for the presense of the expected security headers
     * and create a proxy user to execute further request chain. Responds with an HTTP error if the header is missing
     * or the chain processing throws an exception.
     *
     * @param request http request
     * @param response http response
     * @param chain filter chain
     */
    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {

        if (SecureLogin.isUserImpersonationEnabled()) {

            // retrieve user header and make sure header is present and is not empty
            final String user = ((HttpServletRequest) request).getHeader(USER_HEADER);
            if (user == null) {
                throw new IllegalArgumentException(MISSING_HEADER_ERROR);
            } else if (user.trim().isEmpty()) {
                throw new IllegalArgumentException(EMPTY_HEADER_ERROR);
            }

            // TODO refresh Kerberos token when security is enabled

            // prepare pivileged action to run on behalf of proxy user
            PrivilegedExceptionAction<Boolean> action = new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws IOException, ServletException {
                    LOG.debug("Performing request chain call for proxy user = " + user);
                    chain.doFilter(request, response);
                    return true;
                }
            };

            // create proxy user UGI from the UGI of the logged in user and execute the servlet chain as that user
            try {
                LOG.debug("Creating proxy user for " + user);
                UserGroupInformation proxyUGI = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());;
                proxyUGI.doAs(action);
            } catch (UndeclaredThrowableException ute) {
                // unwrap the real exception thrown by the action
                throw new ServletException(ute.getCause());
            } catch (InterruptedException ie) {
                throw new ServletException(ie);
            }
        } else {
            // no user impersonation is configured
            chain.doFilter(request, response);
        }
    }

    /**
     * Destroys the filter.
     */
    @Override
    public void destroy() {
    }

}
