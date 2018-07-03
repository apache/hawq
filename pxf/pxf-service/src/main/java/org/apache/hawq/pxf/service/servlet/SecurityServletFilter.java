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
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final String SEGMENT_COUNT_HEADER = "X-GP-SEGMENT-COUNT";
    private static final String SEGMENT_ID_HEADER = "X-GP-SEGMENT-ID";
    private static final String TRANSACTION_ID_HEADER = "X-GP-XID";
    private static final String FRAGMENT_INDEX_HEADER = "X-GP-FRAGMENT-INDEX";
    private static final String FRAGMENT_COUNT_HEADER = "X-GP-FRAGMENT-COUNT";
    private static final String MISSING_HEADER_ERROR = "Header %s is missing in the request";
    private static final String EMPTY_HEADER_ERROR = "Header %s is empty in the request";
    private static Map<SegmentTransactionId, TimedProxyUGI> cache = new ConcurrentHashMap<>();
    private static DelayQueue<TimedProxyUGI> delayQueue = new DelayQueue<>();
    private static long UGI_CACHE_EXPIRY = 15 * 60 * 1000L; // 15 Minutes


    private class TimedProxyUGI implements Delayed {
        long startTime;
        UserGroupInformation proxyUGI;
        SegmentTransactionId session;
        AtomicInteger inProgress = new AtomicInteger();

        public TimedProxyUGI(UserGroupInformation proxyUGI, SegmentTransactionId session) {
            this.startTime = System.currentTimeMillis();
            this.proxyUGI = proxyUGI;
            this.session = session;
            inProgress.incrementAndGet();
        }

        public void incrementCounter() {
            inProgress.incrementAndGet();
        }

        public void decrementCounter() {
            inProgress.decrementAndGet();
        }

        public void resetTime() {
            startTime = System.currentTimeMillis();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(getDelayMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            TimedProxyUGI that = (TimedProxyUGI) other;
            return Long.compare(this.getDelayMillis(), that.getDelayMillis());
        }

        private long getDelayMillis() {
            return (startTime + UGI_CACHE_EXPIRY) - System.currentTimeMillis();
        }
    }

    private class SegmentTransactionId {
        String segmentId;
        String transactionId;
        String segmentTransactionId;

        public SegmentTransactionId(String segmentId, String transactionId) {
            this.segmentId = segmentId;
            this.transactionId = transactionId;
            this.segmentTransactionId = segmentId + ":" + transactionId;
        }

        @Override
        public int hashCode() {
            return segmentTransactionId.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof SegmentTransactionId)) {
                return false;
            }
            SegmentTransactionId that = (SegmentTransactionId) other;
            return this.segmentTransactionId.equals(that.segmentTransactionId);
        }

        @Override
        public String toString() {
            return "Session = " + segmentTransactionId;
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
            String segmentId = getHeaderValue(request, SEGMENT_ID_HEADER);
            String transactionId = getHeaderValue(request, TRANSACTION_ID_HEADER);
            Integer fragmentCount = getHeaderValueInt(request, FRAGMENT_COUNT_HEADER, false);
            Integer fragmentIndex = getHeaderValueInt(request, FRAGMENT_INDEX_HEADER, false);

            SegmentTransactionId session = new SegmentTransactionId(segmentId, transactionId);
            if (fragmentCount != null) {
                StringBuilder sb = new StringBuilder(session.toString());
                sb.append(" Fragment = ").append(fragmentIndex).append(" of ").append(fragmentCount);
                LOG.info(sb.toString());
            }

            // TODO refresh Kerberos token when security is enabled

            // prepare privileged action to run on behalf of proxy user
            PrivilegedExceptionAction<Boolean> action = new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws IOException, ServletException {
                    LOG.debug("Performing request chain call for proxy user = " + user);
                    chain.doFilter(request, response);
                    return true;
                }
            };

            // create proxy user UGI from the UGI of the logged in user and execute the servlet chain as that user
            TimedProxyUGI timedProxyUGI = getTimedProxyUGI(user, session);
            try {
                timedProxyUGI.proxyUGI.doAs(action);
            } catch (UndeclaredThrowableException ute) {
                // unwrap the real exception thrown by the action
                throw new ServletException(ute.getCause());
            } catch (InterruptedException ie) {
                throw new ServletException(ie);
            }
            finally {
                release(timedProxyUGI, fragmentIndex, fragmentCount);
            }
        } else {
            // no user impersonation is configured
            chain.doFilter(request, response);
        }
    }

   private TimedProxyUGI getTimedProxyUGI(String user, SegmentTransactionId session) throws IOException {
        synchronized (session.segmentTransactionId.intern()) {
            TimedProxyUGI timedProxyUGI = cache.get(session);
            if (timedProxyUGI == null || timedProxyUGI.getDelayMillis() < 0) {
                cleanup();
                LOG.info(session.toString() + " Creating proxy user = " + user);
                UserGroupInformation proxyUGI =
                        UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
                timedProxyUGI = new TimedProxyUGI(proxyUGI, session);
                delayQueue.offer(timedProxyUGI);
                cache.put(session, timedProxyUGI);
            } else {
                timedProxyUGI.incrementCounter();
            }
            return timedProxyUGI;
        }
    }

    private void release(TimedProxyUGI timedProxyUGI, Integer fragmentIndex, Integer fragmentCount) {
        synchronized (timedProxyUGI.session.segmentTransactionId.intern()) {
            timedProxyUGI.resetTime();
            timedProxyUGI.decrementCounter();
            if (fragmentIndex != null && fragmentCount.equals(fragmentIndex))
                closeUGI(timedProxyUGI);
        }
    }

    private void cleanup() {
        TimedProxyUGI timedProxyUGI = delayQueue.poll();
        while (timedProxyUGI != null) {
            closeUGI(timedProxyUGI);
            LOG.info(timedProxyUGI.session.toString() + " Delay Queue Size = " + delayQueue.size());
            timedProxyUGI = delayQueue.poll();
        }
    }

    private void closeUGI(TimedProxyUGI timedProxyUGI) {
        synchronized (timedProxyUGI.session.segmentTransactionId.intern()) {
            String fsMsg = "FileSystem for proxy user = " + timedProxyUGI.proxyUGI.getUserName();
            try {
                if (timedProxyUGI.inProgress.get() != 0) {
                    LOG.info(timedProxyUGI.session.toString() + " Skipping close of " + fsMsg);
                    timedProxyUGI.resetTime();
                    delayQueue.offer(timedProxyUGI);
                    return;
                }
                if (cache.remove(timedProxyUGI.session) != null) {
                    LOG.info(timedProxyUGI.session.toString() + " Closing " + fsMsg +
                            " (Cache Size = " + cache.size() + ")");
                    FileSystem.closeAllForUGI(timedProxyUGI.proxyUGI);
                }
            } catch (Throwable t) {
                LOG.warn(timedProxyUGI.session.toString() + " Error closing " + fsMsg);
            }
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
