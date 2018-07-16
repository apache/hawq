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
 * have not been accessed for UGI_CACHE_EXPIRY milliseconds.
 * <p>
 * The motivation for caching is that destroying UGIs is slow. The alternative, creating and
 * destroying a UGI per-request, is wasteful.
 */
public class UGICache {

    private static final Log LOG = LogFactory.getLog(UGICache.class);
    private final Map<SessionId, Entry> cache = new ConcurrentHashMap<>();
    // There is a separate DelayQueue for each segment (also being used for locking)
    private final Map<Integer, DelayQueue<Entry>> expirationQueueMap = new HashMap<>();
    private final UGIProvider ugiProvider;
    private final Ticker ticker;
    final static long UGI_CACHE_EXPIRY = 15 * 60 * 1000L; // 15 Minutes

    /**
     * Create a UGICache with the given {@link Ticker} and {@link UGIProvider}. Intended for use by
     * tests which need to mock UGI creation/destruction and the current time.
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
     * If a UGI for the given session exists in the cache, returns it. Otherwise, creates a new
     * proxy UGI. In either case this method increments the reference count of the UGI. This method
     * also destroys expired, unreferenced UGIs for the same segmentId as the given session.
     *
     * @param session The user from the session is impersonated by the proxy UGI.
     * @return the proxy UGI for the given session.
     */
    public UserGroupInformation getUserGroupInformation(SessionId session) throws IOException {
        Integer segmentId = session.getSegmentId();
        String user = session.getUser();
        DelayQueue<Entry> delayQueue = getExpirationQueue(segmentId);
        synchronized (delayQueue) {
            // Use the opportunity to cleanup any expired entries
            cleanup(delayQueue);
            Entry entry = cache.get(session);
            if (entry == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(session + " Creating proxy user = " + user);
                }
                entry = new Entry(ticker, ugiProvider.createProxyUGI(user), session);
                delayQueue.offer(entry);
                cache.put(session, entry);
            }
            entry.incrementRefCount();
            return entry.getUGI();
        }
    }

    /**
     * Decrement reference count for the given session's UGI. Resets the time at which the UGI will
     * expire to UGI_CACHE_EXPIRY milliseconds in the future.
     *
     * @param session                  the session for which we want to release the UGI.
     * @param cleanImmediatelyIfNoRefs if true, destroys the UGI for the given session (only if it
     *                                 is now unreferenced).
     */
    public void release(SessionId session, boolean cleanImmediatelyIfNoRefs) {

        Entry entry = cache.get(session);

        if (entry == null) {
            throw new IllegalStateException("Cannot release UGI for this session; it is not cached: " + session);
        }

        DelayQueue<Entry> expirationQueue = getExpirationQueue(session.getSegmentId());

        synchronized (expirationQueue) {
            entry.decrementRefCount();
            expirationQueue.remove(entry);
            if (cleanImmediatelyIfNoRefs && entry.isNotInUse()) {
                closeUGI(entry);
            } else {
                // Reset expiration time and put it back in the queue
                // only when we don't close the UGI
                entry.resetTime();
                expirationQueue.offer(entry);
            }
        }
    }

    /**
     * @return the size of the cache
     */
    int size() {
        return cache.size();
    }

    /**
     * This method is not thread-safe, and is intended to be called in tests.
     *
     * @return the sum of the sizes of the internal queues
     */
    int allQueuesSize() {
        int count = 0;
        for (DelayQueue queue : expirationQueueMap.values()) {
            count += queue.size();
        }
        return count;
    }

    /**
     * This method is O(n) in the number of cache entries and should only be called in tests.
     *
     * @param session
     * @return determine whether the session is in the internal cache
     */
    boolean contains(SessionId session) {
        DelayQueue<Entry> expirationQueue = getExpirationQueue(session.getSegmentId());
        synchronized (expirationQueue) {
            Entry entry = cache.get(session);
            return entry != null && expirationQueue.contains(entry);
        }
    }

    /**
     * Get the queue of cache entries associated with a segment, creating it if it doesn't yet
     * exist. This lets us lazily populate the expirationQueueMap.
     *
     * @param segmentId
     * @return the {@link DelayQueue} associated to the segment.
     */
    private DelayQueue<Entry> getExpirationQueue(Integer segmentId) {
        DelayQueue<Entry> queue = expirationQueueMap.get(segmentId);
        if (queue == null) {
            synchronized (expirationQueueMap) {
                queue = expirationQueueMap.get(segmentId);
                if (queue == null) {
                    queue = new DelayQueue<>();
                    expirationQueueMap.put(segmentId, queue);
                }
            }
        }
        return queue;
    }

    /**
     * Iterate through all the entries in the queue and close expired {@link UserGroupInformation},
     * otherwise it resets the timer for every non-expired entry.
     *
     * @param expirationQueue
     */
    private void cleanup(DelayQueue<Entry> expirationQueue) {

        Entry expiredUGI;
        while ((expiredUGI = expirationQueue.poll()) != null) {
            if (expiredUGI.isNotInUse()) {
                closeUGI(expiredUGI);
            } else {
                // The UGI object is still being used by another thread
                String fsMsg = "FileSystem for proxy user = " + expiredUGI.getSession().getUser();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(expiredUGI.getSession().toString() + " Skipping close of " + fsMsg);
                }
                // Place it back in the queue if still in use and was not closed
                expiredUGI.resetTime();
                expirationQueue.offer(expiredUGI);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Delay Queue Size for segment " +
                        expiredUGI.getSession().getSegmentId() + " = " + expirationQueue.size());
            }
        }
    }

    /**
     * This method must be called from a synchronized block for the delayQueue for the given
     * session.getSegmentId(). Removes the cachedUGI from the internal cache and then passes it to
     * {@link UGIProvider} to destroy the UGI.
     *
     * @param expiredUGI
     */
    private void closeUGI(Entry expiredUGI) {
        SessionId session = expiredUGI.getSession();
        String fsMsg = "FileSystem for proxy user = " + session.getUser();

        if (LOG.isDebugEnabled()) {
            LOG.debug(session.toString() + " Closing " + fsMsg +
                    " (Cache Size = " + cache.size() + ")");
        }

        try {
            // Remove it from cache, as cache now has an
            // expired entry which is not in progress
            cache.remove(session);
            ugiProvider.destroy(expiredUGI.getUGI());

        } catch (Throwable t) {
            LOG.warn(session.toString() + " Error closing " + fsMsg, t);
        }
    }

    /**
     * Stores a {@link UserGroupInformation}, and determines when to expire the UGI.
     */
    private static class Entry implements Delayed {

        private volatile long startTime;
        private final SessionId session;
        private final UserGroupInformation proxyUGI;
        private final AtomicInteger referenceCount = new AtomicInteger();
        private final Ticker ticker;

        /**
         * Creates a new UGICache Entry.
         *
         * @param ticker
         * @param proxyUGI
         * @param session
         */
        Entry(Ticker ticker, UserGroupInformation proxyUGI, SessionId session) {
            this.ticker = ticker;
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
         * @return the session associated to the {@link UserGroupInformation}.
         */
        public SessionId getSession() {
            return session;
        }

        /**
         * @return true if the UGI is being referenced by a session, false otherwise
         */
        private boolean isNotInUse() {
            return referenceCount.get() <= 0;
        }

        /**
         * Increments the number of references accessing the {@link UserGroupInformation}.
         */
        void incrementRefCount() {
            referenceCount.incrementAndGet();
        }

        /**
         * Decrements the number of references accessing the {@link UserGroupInformation}.
         */
        void decrementRefCount() {
            int count = referenceCount.decrementAndGet();
            if (count < 0) {
                throw new IllegalStateException("UGICache.Entry referenceCount may not be decremented past 0.");
            }
        }

        /**
         * Resets the timer for removing this Entry from the cache.
         */
        void resetTime() {
            startTime = currentTimeMillis();
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
            if (!(other instanceof Entry)) return 1;

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
         * @return the current Unix timestamp in milliseconds (equivalent to {@link
         * System}.currentTimeMillis)
         */
        private long currentTimeMillis() {
            return ticker.read() / 1000;
        }
    }
}
