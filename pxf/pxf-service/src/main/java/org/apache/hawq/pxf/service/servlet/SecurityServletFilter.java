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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hawq.pxf.service.SessionId;
import org.apache.hawq.pxf.service.UGICache;
import org.apache.hawq.pxf.service.utilities.SecureLogin;


/**
 * Listener on lifecycle events of our webapp
 */
public class SecurityServletFilter implements Filter {

    private static final Log LOG = LogFactory.getLog(SecurityServletFilter.class);
    private static final String USER_HEADER = "X-GP-USER";
    private static final String SEGMENT_ID_HEADER = "X-GP-SEGMENT-ID";
    private static final String TRANSACTION_ID_HEADER = "X-GP-XID";
    private static final String FRAGMENT_INDEX_HEADER = "X-GP-FRAGMENT-INDEX";
    private static final String FRAGMENT_COUNT_HEADER = "X-GP-FRAGMENT-COUNT";
    private static final String MISSING_HEADER_ERROR = "Header %s is missing in the request";
    private static final String EMPTY_HEADER_ERROR = "Header %s is empty in the request";
    private static final UGICache cache = new UGICache();

    /**
     * Initializes the filter.
     *
     * @param filterConfig filter configuration
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        //TODO: initialize cache here
    }

    /**
     * If user impersonation is configured, examines the request for the presense of the expected security headers
     * and create a proxy user to execute further request chain. Responds with an HTTP error if the header is missing
     * or the chain processing throws an exception.
     *
     * @param request  http request
     * @param response http response
     * @param chain    filter chain
     */
    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException {

        if (SecureLogin.isUserImpersonationEnabled()) {

            // retrieve user header and make sure header is present and is not empty
            final String gpdbUser = getHeaderValue(request, USER_HEADER);
            String transactionId = getHeaderValue(request, TRANSACTION_ID_HEADER);
            Integer segmentId = getHeaderValueInt(request, SEGMENT_ID_HEADER, true);
            Integer fragmentCount = getHeaderValueInt(request, FRAGMENT_COUNT_HEADER, false);
            Integer fragmentIndex = getHeaderValueInt(request, FRAGMENT_INDEX_HEADER, false);

            SessionId session = new SessionId(segmentId, transactionId, gpdbUser);
            if (LOG.isDebugEnabled() && fragmentCount != null) {
                LOG.debug(session.toString() + " Fragment = " + fragmentIndex + " of " + fragmentCount);
            }

            // TODO refresh Kerberos token when security is enabled

            // prepare privileged action to run on behalf of proxy user
            PrivilegedExceptionAction<Boolean> action = new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws IOException, ServletException {
                    LOG.debug("Performing request chain call for proxy user = " + gpdbUser);
                    chain.doFilter(request, response);
                    return true;
                }
            };

            // create proxy user UGI from the UGI of the logged in user and execute the servlet chain as that user
            UserGroupInformation ugi = cache.getUserGroupInformation(session);
            try {
                ugi.doAs(action);
            } catch (UndeclaredThrowableException ute) {
                // unwrap the real exception thrown by the action
                throw new ServletException(ute.getCause());
            } catch (InterruptedException ie) {
                throw new ServletException(ie);
            } finally {
                // Optimization to cleanup the cache if it is the last fragment
                boolean forceClean = (fragmentIndex != null && fragmentIndex.equals(fragmentCount));
                cache.release(session, forceClean);
            }
        } else {
            // no user impersonation is configured
            chain.doFilter(request, response);
        }
    }

    private Integer getHeaderValueInt(ServletRequest request, String headerKey, boolean required)
            throws IllegalArgumentException {
        String value = getHeaderValue(request, headerKey, required);
        return value != null ? Integer.valueOf(value) : null;
    }

    private String getHeaderValue(ServletRequest request, String headerKey)
            throws IllegalArgumentException {
        return getHeaderValue(request, headerKey, true);
    }

    private String getHeaderValue(ServletRequest request, String headerKey, boolean required)
            throws IllegalArgumentException {
        String value = ((HttpServletRequest) request).getHeader(headerKey);
        if (required && value == null) {
            throw new IllegalArgumentException(String.format(MISSING_HEADER_ERROR, headerKey));
        } else if (required && value.trim().isEmpty()) {
            throw new IllegalArgumentException(String.format(EMPTY_HEADER_ERROR, headerKey));
        }
        return value;
    }

    /**
     * Destroys the filter.
     */
    @Override
    public void destroy() {
    }

}
