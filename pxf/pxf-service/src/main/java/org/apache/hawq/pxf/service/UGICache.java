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

import com.google.common.base.Ticker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Stores UserGroupInformation instances for each active session. The UGIs are cleaned up if they
 * have not been accessed for 15 minutes.
 * <p>
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
    private Ticker ticker;
    private final static long UGI_CACHE_EXPIRY = 15 * 60 * 1000L; // 15 Minutes

    /**
     * Create a UGICache with the given {@link UGIProvider}. Intended for use by tests which need
     * to substitute a mock UGIProvider.
     */
    UGICache(UGIProvider provider, Ticker ticker) {
        this.ticker = ticker;
        this.ugiProvider = provider;
    }

    /**
     * Create a UGICache. Automatically creates a {@link UGIProvider} that this cache will use to
     * create and destroy UserGroupInformation instances.
     */
    public UGICache() {
        this(new UGIProvider(), Ticker.systemTicker());
    }

    /**
     * Create new proxy UGI if not found in cache and increment reference count
     */
    public UserGroupInformation getUserGroupInformation(SessionId session) throws IOException {
        Integer segmentId = session.getSegmentId();
        String user = session.getUser();
        DelayQueue<Entry> delayQueue = getExpirationQueue(segmentId);
        synchronized (delayQueue) {
            // Use the opportunity to cleanup any expired entries
            cleanup(session);
            Entry entry = cache.get(session);
            if (entry == null) {
                LOG.info(session.toString() + " Creating proxy user = " + user);
                entry = new Entry(ticker, ugiProvider.createProxyUGI(user));
                delayQueue.offer(entry);
                cache.put(session, entry);
            }
            entry.acquireReference();
            return entry.getUGI();
        }
    }

    /**
     * Decrement reference count for the given session's UGI. Resets the time at which the UGI will
     * expire to 15 minutes in the future.
     *
     * @param session                  the session for which we want to release the UGI.
     * @param cleanImmediatelyIfNoRefs if true, destroys the UGI for the given session (only if it is
     *                                 now unreferenced).
     */
    public void release(SessionId session, boolean cleanImmediatelyIfNoRefs) {

        Entry timedProxyUGI = cache.get(session);

        if (timedProxyUGI == null) return;

        timedProxyUGI.resetTime();
        timedProxyUGI.releaseReference();
        if (cleanImmediatelyIfNoRefs) {
            synchronized (getExpirationQueue(session.getSegmentId())) {
                timedProxyUGI.setExpired();
                closeUGI(session, timedProxyUGI);
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
     * Iterate through all the entries in the queue for the given session's segment
     * and close expired {@link UserGroupInformation}, otherwise it resets
     * the timer for every non-expired entry.
     *
     * @param session
     */
    private void cleanup(SessionId session) {

        Entry ugi;
        DelayQueue<Entry> delayQueue = getExpirationQueue(session.getSegmentId());
        while ((ugi = delayQueue.poll()) != null) {
            // Place it back in the queue if still in use and was not closed
            if (!closeUGI(session, ugi)) {
                ugi.resetTime();
                delayQueue.offer(ugi);
            }
            LOG.debug("Delay Queue Size for segment " +
                    session.getSegmentId() + " = " + delayQueue.size());
        }
    }

    /**
     * This method must be called from a synchronized block for the delayQueue for the given
     * session.getSegmentId(). When the reference count is 0, the Entry is removed from the
     * cache where it will then be processed to be destroyed by the UGIProvider.
     *
     * @param session
     * @param toDelete
     * @return true if the UGI entry was cleaned, false when the UGI entry was still in use
     * and cleaning up was skipped
     */
    private boolean closeUGI(SessionId session, Entry toDelete) {
        // There is no need to synchronize this method because it should be called
        // from within a synchronized block
        String fsMsg = "FileSystem for proxy user = " + toDelete.getUGI().getUserName();
        try {
            // The UGI object is still being used by another thread
            if (toDelete.isInUse()) {
                LOG.info(session.toString() + " Skipping close of " + fsMsg);
                return false;
            }

            // Expired UGI object can be cleaned since it is not used
            // Determine if it can be removed from cache also
            Entry cachedUGI = cache.get(session);
            if (toDelete == cachedUGI) {
                // Remove it from cache, as cache now has an
                // expired entry which is not in progress
                cache.remove(session);
            }

            // Optimization to call close only if it has not been
            // called for that UGI
            if (!toDelete.isCleaned()) {
                LOG.info(session.toString() + " Closing " + fsMsg +
                        " (Cache Size = " + cache.size() + ")");
                ugiProvider.destroy(toDelete.getUGI());
                toDelete.setCleaned();
            }

        } catch (Throwable t) {
            LOG.warn(session.toString() + " Error closing " + fsMsg, t);
        }
        return true;
    }

    /**
     * Stores a {@link UserGroupInformation}, and determines when to expire the UGI.
     */
    private static class Entry implements Delayed {

        private volatile long startTime;
        private UserGroupInformation proxyUGI;
        private boolean cleaned = false;
        private final AtomicInteger referenceCount = new AtomicInteger();
        private final Ticker ticker;

        /**
         * Creates a new UGICache Entry.
         *
         * @param ticker
         * @param proxyUGI
         */
        Entry(Ticker ticker, UserGroupInformation proxyUGI) {
            this.ticker = ticker;
            this.proxyUGI = proxyUGI;
        }

        /**
         * @return the Cached {@link UserGroupInformation}.
         */
        public UserGroupInformation getUGI() {
            return proxyUGI;
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
         * @return true if the UGI is being referenced by a session, false otherwise
         */
        private boolean isInUse() {
            return referenceCount.get() > 0;
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
            int count = referenceCount.decrementAndGet();
            assert count >= 0;
        }

        /**
         * Resets the timer for removing this Entry from the cache.
         */
        void resetTime() {
            startTime = currentTimeMillis();
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
            return (startTime + UGI_CACHE_EXPIRY) - currentTimeMillis();
        }

        /**
         * @return the current Unix timestamp in milliseconds (equivalent to {@link System}.currentTimeMillis)
         */
        private long currentTimeMillis() {
            return ticker.read() / 1000;
        }
    }
}
