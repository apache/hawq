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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hawq.pxf.service.utilities.SecureLogin;


/**
 * Listener on lifecycle events of our webapp
 */
public class SecurityServletFilter implements Filter {

    private static final Log LOG = LogFactory.getLog(SecurityServletFilter.class);
    private static final String USER_HEADER = "X-GP-USER";
    private static final String SEGMENT_INDEX_HEADER = "X-GP-SEGMENT-ID";
    private static final String TRANSACTION_ID_HEADER = "X-GP-XID";
    private static final String MISSING_HEADER_ERROR = "Header %s is missing in the request";
    private static final String EMPTY_HEADER_ERROR = "Header %s is empty in the request";
    private static Map<SegmentTransactionId, TimedProxyUGI> cache = new ConcurrentHashMap<>();
    private static DelayQueue<TimedProxyUGI> delayQueue = new DelayQueue<>();
    private static long UGI_CACHE_EXPIRY = 15 * 60 * 1000L; // 15 Minutes


    private static class TimedProxyUGI implements Delayed {
        long startTime;
        UserGroupInformation proxyUGI;
        SegmentTransactionId session;

        public TimedProxyUGI(UserGroupInformation proxyUGI, SegmentTransactionId session) {
            this.startTime = System.currentTimeMillis();
            this.proxyUGI = proxyUGI;
            this.session = session;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(getDelayMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(this.getDelayMillis(), ((TimedProxyUGI)other).getDelayMillis());
        }

        private long getDelayMillis() {
            return (startTime + UGI_CACHE_EXPIRY) - System.currentTimeMillis();
        }
    }

    private static class SegmentTransactionId {
        String segmentIndex;
        String transactionId;

        public SegmentTransactionId(String segmentIndex, String transactionId) {
            this.segmentIndex = segmentIndex;
            this.transactionId = transactionId;
        }

    }

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
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
            throws IOException, ServletException {

        if (SecureLogin.isUserImpersonationEnabled()) {

            // retrieve user header and make sure header is present and is not empty
            final String user = getHeaderValue(request, USER_HEADER);
            final String segmentIndex = getHeaderValue(request, SEGMENT_INDEX_HEADER);
            final String transactionId = getHeaderValue(request, TRANSACTION_ID_HEADER);

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
            SegmentTransactionId session = new SegmentTransactionId(segmentIndex, transactionId);
            TimedProxyUGI timedProxyUGI = cache.get(session);
            if (timedProxyUGI == null || timedProxyUGI.getDelayMillis() > 0) {
                cleanup();
                LOG.debug("Creating proxy user = " + user);
                UserGroupInformation proxyUGI =
                        UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
                timedProxyUGI = new TimedProxyUGI(proxyUGI, session);
                delayQueue.offer(timedProxyUGI);
                cache.put(session, timedProxyUGI);
            }
            try {
                timedProxyUGI.proxyUGI.doAs(action);
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

    private void cleanup() {
        TimedProxyUGI timedProxyUGI = delayQueue.poll();
        while (timedProxyUGI != null) {
            try {
                LOG.debug("Closing FileSystem for proxy user = " + timedProxyUGI.proxyUGI.getUserName());
                FileSystem.closeAllForUGI(timedProxyUGI.proxyUGI);
                cache.remove(timedProxyUGI.session);
            } catch (Throwable t) {
                LOG.warn("Error closing FileSystem for proxy user = " + timedProxyUGI.proxyUGI.getUserName());
            }
            timedProxyUGI = delayQueue.poll();
        }
    }

    private String getHeaderValue(ServletRequest request, String headerKey) throws IllegalArgumentException {
        String value = ((HttpServletRequest) request).getHeader(headerKey);
        if (value == null) {
            throw new IllegalArgumentException(String.format(MISSING_HEADER_ERROR, headerKey));
        } else if (value.trim().isEmpty()) {
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
