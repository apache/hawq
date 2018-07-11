package org.apache.hawq.pxf.service;

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Stores UserGroupInformation instances for each active session. The UGIs are cleaned up if they
 * have not been accessed for 15 minutes.
 *
 * The motivation for caching is that destroying UGIs is slow. The alternative, creating and
 * destroying a UGI per-request, is wasteful.
 */
public class UGICache {

    private static final Log LOG = LogFactory.getLog(UGICache.class);
    private Map<SessionId, Entry> cache = new ConcurrentHashMap<>();
    @SuppressWarnings("unchecked")
    // There is a separate DelayQueue for each segment (also being used for locking)
    private final Map<Integer, DelayQueue<Entry>> queueMap = new HashMap<>();
    private final UGIProvider ugiProvider;

    /**
     * Create a UGICache with the given {@link UGIProvider}. Intended for use by tests which need
     * to substitute a mock UGIProvider.
     */
    UGICache(UGIProvider provider) {
        this.ugiProvider = provider;
    }

    /**
     * Create a UGICache. Automatically creates a {@link UGIProvider} that this cache will use to
     * create and destroy UserGroupInformation instances.
     */
    public UGICache() {
        this(new UGIProvider());
    }

    /**
     * Create new proxy UGI if not found in cache and increment reference count
     */
    public Entry getTimedProxyUGI(SessionId session)
            throws IOException {

        Integer segmentId = session.getSegmentId();
        String user = session.getUser();
        DelayQueue<Entry> delayQueue = getExpirationQueue(segmentId);
        synchronized (delayQueue) {
            // Use the opportunity to cleanup any expired entries
            cleanup(segmentId);
            Entry entry = cache.get(session);
            if (entry == null) {
                LOG.info(session.toString() + " Creating proxy user = " + user);
                entry = new Entry(ugiProvider.createProxyUGI(user), session);
                delayQueue.offer(entry);
                cache.put(session, entry);
            }
            entry.acquireReference();
            return entry;
        }
    }

    /**
     * Decrement reference count for the given Entry.
     *
     * @param timedProxyUGI the cache entry to release
     * @param forceClean if true, destroys the UGI for the given Entry (only if it is now unreferenced).
     */
    public void release(Entry timedProxyUGI, boolean forceClean) {

        Integer segmentId = timedProxyUGI.getSession().getSegmentId();
        timedProxyUGI.resetTime();
        timedProxyUGI.releaseReference();
        if (forceClean) {
            synchronized (getExpirationQueue(segmentId)) {
                timedProxyUGI.setExpired();
                closeUGI(timedProxyUGI);
            }
        }
    }

    /**
     * Get the queue of cache entries associated with a segment, creating it if it doesn't
     * yet exist. This lets us lazily populate the queueMap.
     *
     * @param segmentId
     * @return the {@link DelayQueue} associated to the segment.
     */
    private DelayQueue<Entry> getExpirationQueue(Integer segmentId) {
        DelayQueue<Entry> queue = queueMap.get(segmentId);
        if (queue == null) {
            synchronized (queueMap) {
                queue = queueMap.get(segmentId);
                if (queue == null) {
                    queue = new DelayQueue<>();
                    queueMap.put(segmentId, queue);
                }
            }
        }
        return queue;
    }

    /**
     * Iterate through all the entries in the queue for the given segment
     * and close expired {@link UserGroupInformation}, otherwise it resets
     * the timer for every non-expired entry.
     *
     * @param segmentId
     */
    private void cleanup(Integer segmentId) {

        Entry ugi;
        DelayQueue<Entry> delayQueue = getExpirationQueue(segmentId);
        while ((ugi = delayQueue.poll()) != null) {
            // Place it back in the queue if still in use and was not closed
            if (!closeUGI(ugi)) {
                ugi.resetTime();
                delayQueue.offer(ugi);
            }
            LOG.debug("Delay Queue Size for segment " +
                    segmentId + " = " + delayQueue.size());
        }
    }

    // There is no need to synchronize this method because it should be called
    // from within a synchronized block
    private boolean closeUGI(Entry expiredProxyUGI) {

        SessionId session = expiredProxyUGI.getSession();
        String fsMsg = "FileSystem for proxy user = " + expiredProxyUGI.getUGI().getUserName();
        try {
            // The UGI object is still being used by another thread
            if (expiredProxyUGI.countReferences() != 0) {
                LOG.info(session.toString() + " Skipping close of " + fsMsg);
                // Reset time so that it doesn't expire until release
                // updates the time again
//                expiredProxyUGI.resetTime();
                return false;
            }

            // Expired UGI object can be cleaned since it is not used
            // Determine if it can be removed from cache also
            Entry cachedUGI = cache.get(session);
            if (expiredProxyUGI == cachedUGI) {
                // Remove it from cache, as cache now has an
                // expired entry which is not in progress
                cache.remove(session);
            }

            // Optimization to call close only if it has not been
            // called for that UGI
            if (!expiredProxyUGI.isCleaned()) {
                LOG.info(session.toString() + " Closing " + fsMsg +
                        " (Cache Size = " + cache.size() + ")");
                ugiProvider.destroy(expiredProxyUGI.getUGI());
                expiredProxyUGI.setCleaned();
            }

        } catch (Throwable t) {
            LOG.warn(session.toString() + " Error closing " + fsMsg);
        }
        return true;
    }

    /**
     * Stores a {@link UserGroupInformation} associated with a {@link SessionId}, and determines
     * when to expire the UGI.
     */
    public static class Entry implements Delayed {

        private volatile long startTime;
        private UserGroupInformation proxyUGI;
        private SessionId session;
        private boolean cleaned = false;
        private AtomicInteger referenceCount = new AtomicInteger();
        private static long UGI_CACHE_EXPIRY = 15 * 60 * 1000L; // 15 Minutes

        /**
         * Creates a new UGICache Entry.
         *
         * @param proxyUGI
         * @param session
         */
        Entry(UserGroupInformation proxyUGI, SessionId session) {
            this.startTime = System.currentTimeMillis();
            this.proxyUGI = proxyUGI;
            this.session = session;
        }

        /**
         * @return the Cached {@link UserGroupInformation}.
         */
        public UserGroupInformation getUGI() {
            return proxyUGI;
        }

        /**
         * @return the {@link SessionId}.
         */
        SessionId getSession() {
            return session;
        }

        /**
         * @return true if the setCleaned has been invoked, false otherwise.
         */
        boolean isCleaned() {
            return cleaned;
        }

        /**
         * mark that the {@link UserGroupInformation} in this Entry has been destroyed.
         */
        void setCleaned() {
            cleaned = true;
        }

        /**
         * @return the number of active requests using the {@link UserGroupInformation}.
         */
        int countReferences() {
            return referenceCount.get();
        }

        /**
         * Increments the number of references accessing the {@link UserGroupInformation}.
         */
        void acquireReference() {
            referenceCount.incrementAndGet();
        }

        /**
         * Decrements the number of references accessing the {@link UserGroupInformation}.
         */
        void releaseReference() {
            referenceCount.decrementAndGet();
        }

        /**
         * Resets the timer for removing this Entry from the cache.
         */
        void resetTime() {
            startTime = System.currentTimeMillis();
        }

        /**
         * Expire the UGICache Entry
         */
        void setExpired() {
            startTime = -1L;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(getDelayMillis(), TimeUnit.MILLISECONDS);
        }

        /**
         * Compare the expiry time of this cache entry to another cache entry's expiry time.
         *
         * @param other a UGICache.Entry (passing any other kind of Delayed produces an error)
         * @see java.lang.Comparable<>#compareTo(java.lang.Comparable<>)
         */
        @Override
        public int compareTo(Delayed other) {
            Entry that = (Entry) other;
            return Long.compare(this.getDelayMillis(), that.getDelayMillis());
        }

        /**
         * @return the number of milliseconds remaining before this cache entry expires.
         */
        private long getDelayMillis() {
            return (startTime + UGI_CACHE_EXPIRY) - System.currentTimeMillis();
        }
    }
}
