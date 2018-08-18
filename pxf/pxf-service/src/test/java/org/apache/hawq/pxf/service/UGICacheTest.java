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

import com.google.common.base.Ticker;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

public class UGICacheTest {
    private static final long MINUTES = 60 * 1000L;
    private UGIProvider provider = null;
    private SessionId session = null;
    private UGICache cache = null;
    private FakeTicker fakeTicker;

    static class FakeTicker extends Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        long advanceTime(long milliseconds) {
            return nanos.addAndGet(milliseconds * UGICache.NANOS_PER_MILLIS) / UGICache.NANOS_PER_MILLIS;
        }
    }

    @Before
    public void setUp() throws Exception {
        provider = mock(UGIProvider.class);
        when(provider.createProxyUGI(any(String.class))).thenAnswer(new Answer<UserGroupInformation>() {
            @Override
            public UserGroupInformation answer(InvocationOnMock invocation) {
                return mock(UserGroupInformation.class);
            }
        });

        session = new SessionId(0, "txn-id", "the-user");
        fakeTicker = new FakeTicker();
        cache = new UGICache(provider, fakeTicker);
    }

    @Test
    public void getUGIFromEmptyCache() throws Exception {
        UserGroupInformation ugi = cache.getUserGroupInformation(session);
        assertNotNull(ugi);
        verify(provider).createProxyUGI("the-user");
    }

    @Test
    public void getSameUGITwiceUsesCache() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);
        assertEquals(ugi1, ugi2);
        verify(provider, times(1)).createProxyUGI("the-user");
        assertCacheSize(1);
    }

    @Test
    public void getUGIWithEquivalentSessionsReturnsTheSameInstance() throws Exception {
        SessionId session2 = new SessionId(0, "txn-id", "the-user");
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session2);
        assertEquals(ugi1, ugi2);
    }

    @Test
    public void getTwoUGIsWithDifferentTransactionsForSameUser() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id-2", "the-user");
        UserGroupInformation proxyUGI1 = cache.getUserGroupInformation(session);
        UserGroupInformation proxyUGI2 = cache.getUserGroupInformation(otherSession);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(2)).createProxyUGI("the-user");
        assertCacheSize(2);
        // getting a new UGI instance for each transaction ID is not strictly necessary, but allows
        // us to expire UGIs for transactions that have finished. If we reused one UGI per user,
        // it might never get to expire from the cache, and eventually Kerberos might invalidate
        // the UGI on its end.
    }

    @Test
    public void getTwoUGIsWithDifferentUsers() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id", "different-user");
        UserGroupInformation proxyUGI1 = cache.getUserGroupInformation(session);
        UserGroupInformation proxyUGI2 = cache.getUserGroupInformation(otherSession);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, times(1)).createProxyUGI("different-user");
        assertCacheSize(2);
        assertStillInCache(session, proxyUGI1);
        assertStillInCache(otherSession, proxyUGI2);
    }

    @Test
    public void anySegmentIdIsValid() throws Exception {
        int crazySegId = Integer.MAX_VALUE;
        session = new SessionId(crazySegId, "txn-id", "the-user");
        UserGroupInformation proxyUGI1 = cache.getUserGroupInformation(session);
        assertNotNull(proxyUGI1);
        assertStillInCache(session, proxyUGI1);
    }

    @Test
    public void ensureCleanUpAfterExpiration() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        cache.release(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);

        SessionId session2 = new SessionId(0, "txn-id", "the-user-2");
        cache.getUserGroupInformation(session2); // this triggers cleanup of ugi1
        assertNoLongerInCache(session, ugi1);
        cache.release(session2, true);
        assertCacheSize(0);
    }

    @Test
    public void ensureExpiredUGIIsNotCleanedUpIfItIsStillReferenced() throws Exception {
        SessionId session2 = new SessionId(0, "txn-id", "the-user-2");
        UserGroupInformation stillInUse = cache.getUserGroupInformation(session);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);

        // at this point, stillInUse is expired but still in use
        cache.getUserGroupInformation(session2); // trigger cleanup
        assertStillInCache(session, stillInUse);
        cache.release(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);

        cache.getUserGroupInformation(session2);

        verify(provider, times(1)).destroy(stillInUse);
    }

    @Test
    public void putsItemsBackInTheQueueWhenResettingExpirationDate() throws Exception {
        SessionId session2 = new SessionId(0, "txn-id", "the-user-2");
        SessionId session3 = new SessionId(0, "txn-id", "the-user-3");

        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY - 1000);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session2);
        cache.release(session2, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY - 1000);
        cache.release(session, false);
        fakeTicker.advanceTime(2 * MINUTES);
        cache.getUserGroupInformation(session3);

        assertStillInCache(session, ugi1);
        assertNoLongerInCache(session2, ugi2);
    }

    @Test
    public void releaseWithoutImmediateCleanup() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);

        cache.release(session, false);
        assertStillInCache(session, ugi1);
    }

    @Test
    public void releaseWithImmediateCleanup() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);

        cache.release(session, true);
        assertNoLongerInCache(session, ugi1);
    }

    @Test
    public void releaseWithImmediateCleanupOnlyCleansUGIsForThatSegment() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);

        SessionId differentSeg = new SessionId(999, "txn-id", "user");
        UserGroupInformation ugi2 = cache.getUserGroupInformation(differentSeg);

        cache.release(differentSeg, false); // ugi2 is now unreferenced
        cache.release(session, true);
        assertNoLongerInCache(session, ugi1);
        assertStillInCache(differentSeg, ugi2);
        assertCacheSize(1);
    }

    @Test
    public void releaseResetsTheExpirationTime() throws Exception {
        UserGroupInformation reference1 = cache.getUserGroupInformation(session);
        cache.getUserGroupInformation(session);

        cache.release(session, true);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);
        cache.release(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY - 1000);

        assertStillInCache(session, reference1);
    }

    @Test
    public void releaseAnExpiredUGIResetsTheTimer() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);
        cache.release(session, false);

        assertStillInCache(session, ugi1);

        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY - 1000);
        SessionId session2 = new SessionId(0, "txn-id", "the-user-2");
        cache.getUserGroupInformation(session2); // triggers cleanup
        assertStillInCache(session, ugi1);
    }

    @Test
    public void releaseAndReacquireDoesNotFreeResources() throws Exception {
        cache.getUserGroupInformation(session);
        cache.release(session, false);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);
        UserGroupInformation ugi3 = cache.getUserGroupInformation(session);
        // this does not clean up any UGIs because our ugi is still in use.
        assertEquals(ugi3, ugi2);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, never()).destroy(any(UserGroupInformation.class));
        assertStillInCache(session, ugi2);
    }

    @Test
    public void releaseAndAcquireAfterTimeoutFreesResources() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);

        cache.release(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);
        assertStillInCache(session, ugi1);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);
        verify(provider).destroy(ugi1);
        assertNotEquals(ugi2, ugi1);
        assertStillInCache(session, ugi2);
    }

    @Test
    public void releaseDoesNotFreeResourcesIfUGIIsUsedElsewhere() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        cache.getUserGroupInformation(session); // increments ref count to 2

        cache.release(session, true);
        fakeTicker.advanceTime(60 * MINUTES);
        // UGI was not cleaned up because we are still holding a reference
        assertStillInCache(session, ugi1);
    }

    @Test
    public void releasingAllReferencesFreesResources() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);

        assertEquals(ugi1, ugi2);

        cache.release(session, true);
        assertStillInCache(session, ugi1);
        cache.release(session, true);
        // at this point, the initial UGI has been freed.
        assertNoLongerInCache(session, ugi1);
    }

    @Test(expected = IOException.class)
    public void errorsThrownByCreatingAUgiAreNotCaught() throws Exception {
        when(provider.createProxyUGI("the-user")).thenThrow(new IOException("test exception"));
        cache.getUserGroupInformation(session);
    }

    @Test
    public void errorsThrownByDestroyingAUgiAreCaught() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        doThrow(new IOException("test exception")).when(provider).destroy(ugi1);
        cache.release(session, true); // does not throw
    }

    @Test(expected = IllegalStateException.class)
    public void releaseAnEntryNotInTheCache() {
        // this could happen if some caller of the cache calls release twice.
        cache.release(session, false);
    }

    private void assertStillInCache(SessionId session, UserGroupInformation ugi) throws Exception {
        assertTrue(cache.contains(session));
        verify(provider, never()).destroy(ugi);
    }

    private void assertNoLongerInCache(SessionId session, UserGroupInformation ugi) throws Exception {
        assertFalse(cache.contains(session));
        verify(provider, times(1)).destroy(ugi);
    }

    private void assertCacheSize(int expectedSize) {
        assertEquals(expectedSize, cache.size());
        assertEquals(expectedSize, cache.allQueuesSize());
    }
}
