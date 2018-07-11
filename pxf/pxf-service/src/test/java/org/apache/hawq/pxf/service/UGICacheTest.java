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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
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

    private static class FakeTicker extends Ticker {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }

        public long advanceTime(long milliseconds) {
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
        UserGroupInformation proxyUGI1a = cache.getUserGroupInformation(session);
        UserGroupInformation proxyUGI1b = cache.getUserGroupInformation(session);
        UserGroupInformation proxyUGI2a = cache.getUserGroupInformation(otherSession);
        UserGroupInformation proxyUGI2b = cache.getUserGroupInformation(otherSession);
        assertEquals(proxyUGI1a, proxyUGI1b);
        assertEquals(proxyUGI2a, proxyUGI2b);
        assertNotEquals(proxyUGI1a, proxyUGI2a);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, times(1)).createProxyUGI("different-user");
    }

    @Test
    @Ignore
    public void getUGIWhenRequestedUserDoesNotExist() throws Exception {
        // TODO: what does UserGroupInformation.createProxyUser() do in this scenario?
        // how about getLoginUser()?
    }

    @Test
    public void anySegmentIdIsValid() throws Exception {
        session = new SessionId(65, "txn-id", "the-user");
        UserGroupInformation proxyUGI1 = cache.getUserGroupInformation(session);
        assertNotNull(proxyUGI1);
    }

    @Test
    public void releaseWithoutForceClean() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);

        cache.release(session, false);
        // UGI wasn't cleaned up, so we can still get it
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);
        assertEquals(ugi1, ugi2);
        verify(provider, times(1)).createProxyUGI("the-user");
    }

    @Test
    public void releaseWithForceClean() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);

        cache.release(session, true);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);
        assertNotEquals(ugi1, ugi2);
        verify(provider, times(2)).createProxyUGI("the-user");
    }

    @Test
    public void releaseResetsTheExpirationTime() throws Exception {
        UserGroupInformation reference1 = cache.getUserGroupInformation(session);
        UserGroupInformation reference2 = cache.getUserGroupInformation(session);

        cache.release(session, true);
        fakeTicker.advanceTime(15 * MINUTES);
        cache.release(session, false);
        fakeTicker.advanceTime(10 * MINUTES);

        UserGroupInformation reference3 = cache.getUserGroupInformation(session);

        assertEquals(reference1, reference2);
        assertEquals(reference1, reference3);
        verify(provider, never()).destroy(any(UserGroupInformation.class));
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
    }

    @Test
    public void releaseDoesNotFreeResourcesIfUGIIsUsedElsewhere() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);

        cache.release(session, true);
        fakeTicker.advanceTime(60 * MINUTES);
        // UGI was not cleaned up because proxyUGI2 is referencing it
        UserGroupInformation ugi3 = cache.getUserGroupInformation(session);
        assertEquals(ugi1, ugi2);
        assertEquals(ugi1, ugi3);
        verify(provider, times(1)).createProxyUGI("the-user");
    }

    @Test
    public void releasingAllReferencesFreesResources() throws Exception {
        UserGroupInformation ugi1 = cache.getUserGroupInformation(session);
        UserGroupInformation ugi2 = cache.getUserGroupInformation(session);

        cache.release(session, true);
        verify(provider, never()).destroy(any(UserGroupInformation.class));
        cache.release(session, true);
        verify(provider, times(1)).destroy(any(UserGroupInformation.class));

        // at this point, the initial UGI has been freed. Calling
        // getUserGroupInformation again creates a new UGI.
        UserGroupInformation proxyUGI3 = cache.getUserGroupInformation(session);
        assertEquals(ugi1, ugi2);
        assertNotEquals(ugi1, proxyUGI3);
        verify(provider, times(2)).createProxyUGI("the-user");
    }

    @Test
    public void releaseAnEntryNotInTheCache() {
        // this could happen if some caller of the cache calls release twice.

        cache.release(session, false); // does not throw
    }
}
