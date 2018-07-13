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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
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
            return nanos.addAndGet(milliseconds * 1000) / 1000;
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
    }

    @Test
    public void getTwoUGIsWithDifferentSessionsForSameUser() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id-2", "the-user");
        UserGroupInformation proxyUGI1 = cache.getUserGroupInformation(session);
        UserGroupInformation proxyUGI2 = cache.getUserGroupInformation(otherSession);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(2)).createProxyUGI("the-user");
        // TODO: this seems weird. We're creating two UGIs with the same params,
        // even though we have two different sessions. Why?
    }

    @Test
    public void getTwoUGIsWithDifferentUsers() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id", "different-user");
        UserGroupInformation proxyUGI1 = cache.getUserGroupInformation(session);
        UserGroupInformation proxyUGI2 = cache.getUserGroupInformation(otherSession);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, times(1)).createProxyUGI("different-user");
    }

    @Test
    public void gettingTwoUGIsWithDifferentUsersCachesBoth() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id", "different-user");
        UserGroupInformation ugi1RefA = cache.getUserGroupInformation(session);
        UserGroupInformation ugi1RefB = cache.getUserGroupInformation(session);
        UserGroupInformation ugi2RefA = cache.getUserGroupInformation(otherSession);
        UserGroupInformation ugi2RefB = cache.getUserGroupInformation(otherSession);
        assertSame(ugi1RefA, ugi1RefB);
        assertSame(ugi2RefA, ugi2RefB);
        assertNotEquals(ugi1RefA, ugi2RefA);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, times(1)).createProxyUGI("different-user");
    }

    @Test
    public void anySegmentIdIsValid() throws Exception {
        int crazySegId = Integer.MAX_VALUE;
        session = new SessionId(crazySegId, "txn-id", "the-user");
        UserGroupInformation proxyUGI1 = cache.getUserGroupInformation(session);
        assertNotNull(proxyUGI1);
    }

    @Test
    public void ensureCleanUpAfterExpiration() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);

        fakeTicker.advanceTime(10 * MINUTES);
        cache.release(session, false);
        fakeTicker.advanceTime(16 * MINUTES);
        assertNotInCache(session, ugi1);
    }

    @Test
    public void anUnusedUGIIsFreedAfterDelayWhenAnotherUGIForTheSameSegmentIsAccessed() throws Exception {
        SessionId session2 = new SessionId(0, "txn-id", "the-user-2");
        UserGroupInformation notInUse = cache.getUserGroupInformation(session);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);

        // at this point, notInUse is expired but still in use
        cache.getUserGroupInformation(session2);
        cache.release(session, false);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY + 1000);

        cache.getUserGroupInformation(session2);

        verify(provider, times(1)).destroy(notInUse);
    }

    @Test
    public void putsItemsBackInTheQueueWhenResettingExpirationDate() throws Exception {
        SessionId session2 = new SessionId(0, "txn-id", "the-user-2");
        SessionId session3 = new SessionId(0, "txn-id", "the-user-3");

        cache.getUserGroupInformation(session);
        fakeTicker.advanceTime(10 * MINUTES);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session2);
        cache.release(session2, false);
        fakeTicker.advanceTime(14 * MINUTES);
        cache.release(session, false);
        fakeTicker.advanceTime(2 * MINUTES);
        cache.getUserGroupInformation(session3);

        verify(provider, times(1)).destroy(ugi2);
    }

    @Test
    public void releaseWithoutForceClean() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);

        cache.release(session, false);
        // UGI wasn't cleaned up, so we can still get it
        assertStillInCache(session, ugi1);
        verify(provider, times(1)).createProxyUGI("the-user");
    }

    @Test
    public void releaseWithForceClean() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);

        cache.release(session, true);
        assertNotInCache(session, ugi1);
    }

    @Test
    public void releaseResetsTheExpirationTime() throws Exception {
        UserGroupInformation reference1 = cache.getUserGroupInformation(session);
        cache.getUserGroupInformation(session);

        cache.release(session, true);
        fakeTicker.advanceTime(UGICache.UGI_CACHE_EXPIRY);
        cache.release(session, false);
        fakeTicker.advanceTime(10 * MINUTES);

        assertStillInCache(session, reference1);
    }

    @Test
    public void releaseAndReacquireDoesNotFreeResources() throws Exception {
        cache.getUserGroupInformation(session);
        cache.release(session, false);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);
        fakeTicker.advanceTime(50 * MINUTES);
        UserGroupInformation ugi3 = cache.getUserGroupInformation(session);
        // this does not clean up any UGIs because our ugi is still in use.
        assertEquals(ugi3, ugi2);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, never()).destroy(any(UserGroupInformation.class));
    }

    @Test
    public void releaseAndAcquireAfterTimeoutFreesResources() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);

        cache.release(session, false);
        fakeTicker.advanceTime(20 * MINUTES);
        verify(provider, never()).destroy(any(UserGroupInformation.class));
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);
        verify(provider).destroy(ugi1);
        assertNotEquals(ugi2, ugi1);
        verify(provider, never()).destroy(ugi2);
        // this does not clean up any UGIs because our proxyUGI is still in use.
        verify(provider, times(2)).createProxyUGI("the-user");
    }

    @Test
    public void releaseAnExpiredUGIResetsTheTimer() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        fakeTicker.advanceTime(20 * MINUTES);
        cache.release(session, false);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);
        assertEquals(ugi2, ugi1);
        verify(provider, never()).destroy(any(UserGroupInformation.class));
        // TODO: can we assert that the timer was reset
    }

    @Test
    public void releaseDoesNotFreeResourcesIfUGIIsUsedElsewhere() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        cache.getUserGroupInformation(session);

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
        verify(provider, never()).destroy(any(UserGroupInformation.class));
        cache.release(session, true);
        verify(provider, times(1)).destroy(any(UserGroupInformation.class));

        // at this point, the initial UGI has been freed. Calling
        // getUserGroupInformation again creates a new UGI.
        assertNotInCache(session, ugi1);
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
        UserGroupInformation fromCache = cache.getUserGroupInformation(session);
        assertSame(ugi, fromCache);
        cache.release(session, false);
        verify(provider, never()).destroy(ugi);
    }

    private void assertNotInCache(SessionId session, UserGroupInformation ugi) throws Exception {
        UserGroupInformation fromCache = cache.getUserGroupInformation(session);
        assertNotSame(ugi, fromCache);
        cache.release(session, false);
        verify(provider, times(1)).destroy(ugi);
    }
}
