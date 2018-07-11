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

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

public class UGICacheTest {
    private UGIProvider provider = null;
    private SessionId session = null;
    private UGICache cache = null;

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
        cache = new UGICache(provider);
    }

    @Test
    public void getUGIFromEmptyCache() throws Exception {
        UGICache.Entry entry = cache.getTimedProxyUGI(session);
        assertNotNull(entry.getUGI());
        verify(provider).createProxyUGI("the-user");
    }

    @Test
    public void getSameUGITwiceUsesCache() throws Exception {
        UGICache.Entry entry1 = cache.getTimedProxyUGI(session);
        UGICache.Entry entry2 = cache.getTimedProxyUGI(session);
        assertEquals(entry1, entry2);
        assertNotNull(entry1.getUGI());
        verify(provider, times(1)).createProxyUGI("the-user");
    }

    @Test
    public void getTwoUGIsWithDifferentSessionsForSameUser() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id-2", "the-user");
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);
        UGICache.Entry proxyUGI2 = cache.getTimedProxyUGI(otherSession);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(2)).createProxyUGI("the-user");
        // TODO: this seems weird. We're creating two UGIs with the same params,
        // even though we have two different sessions. Why?
    }

    @Test
    public void getTwoUGIsWithDifferentUsers() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id", "different-user");
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);
        UGICache.Entry proxyUGI2 = cache.getTimedProxyUGI(otherSession);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, times(1)).createProxyUGI("different-user");
    }

    @Test
    public void getTwoUGIsWithDifferentUsersCachesBoth() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id", "different-user");
        UGICache.Entry proxyUGI1a = cache.getTimedProxyUGI(session);
        UGICache.Entry proxyUGI1b = cache.getTimedProxyUGI(session);
        UGICache.Entry proxyUGI2a = cache.getTimedProxyUGI(otherSession);
        UGICache.Entry proxyUGI2b = cache.getTimedProxyUGI(otherSession);
        assertEquals(proxyUGI1a, proxyUGI1b);
        assertEquals(proxyUGI2a, proxyUGI2b);
        assertNotEquals(proxyUGI1a, proxyUGI2a);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, times(1)).createProxyUGI("different-user");
    }

    @Test
    public void getUGIWhenRequestedUserDoesNotExist() throws Exception {
        // what does UserGroupInformation.createProxyUser() do in this scenario?
        // how about getLoginUser()?
    }

    @Test
    public void anySegmentIdIsValid() throws Exception {
        session = new SessionId(65, "txn-id", "the-user");
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);
        assertNotNull(proxyUGI1.getUGI());
    }

    @Test
    public void releaseWithoutForceClean() throws Exception {
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, false);
        // UGI wasn't cleaned up, so we can still get it
        UGICache.Entry proxyUGI2 = cache.getTimedProxyUGI(session);
        assertEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(1)).createProxyUGI("the-user");
    }

    @Test
    public void releaseWithForceClean() throws Exception {
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, true);
        UGICache.Entry proxyUGI2 = cache.getTimedProxyUGI(session);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(2)).createProxyUGI("the-user");
    }

    @Test
    public void releaseWithForceCleanResetsTheExpirationTimeIfUGIIsReferenced() throws Exception {
        UGICache.Entry reference1 = cache.getTimedProxyUGI(session);
        UGICache.Entry reference2 = cache.getTimedProxyUGI(session);

        cache.release(reference1, true);
        cache.release(reference2, false);

        UGICache.Entry reference3 = cache.getTimedProxyUGI(session);

        assertEquals(reference1, reference3);
    }

    @Test
    public void releaseAndReacquireDoesNotFreeResources() throws Exception {
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, false);
        UGICache.Entry proxyUGI2 = cache.getTimedProxyUGI(session);
        proxyUGI2.setExpired();
        UGICache.Entry proxyUGI3 = cache.getTimedProxyUGI(session);
        // this does not clean up any UGIs because our proxyUGI is still in use.
        assertEquals(proxyUGI3, proxyUGI2);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, never()).destroy(any(UserGroupInformation.class));
    }

    @Test
    public void releaseAndAcquireAfterTimeoutFreesResources() throws Exception {
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, false);
        proxyUGI1.setExpired();
        verify(provider, never()).destroy(any(UserGroupInformation.class));
        UGICache.Entry proxyUGI2 = cache.getTimedProxyUGI(session);
        verify(provider).destroy(proxyUGI1.getUGI());
        assertNotEquals(proxyUGI2, proxyUGI1);
        assertNotEquals(proxyUGI2.getUGI(), proxyUGI1.getUGI());
        verify(provider, never()).destroy(proxyUGI2.getUGI());
        // this does not clean up any UGIs because our proxyUGI is still in use.
        assertNotEquals(proxyUGI2, proxyUGI1);
        verify(provider, times(2)).createProxyUGI("the-user");
    }

    @Test
    public void releaseAnExpiredUGIResetsTheTimer() throws Exception {
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);
        proxyUGI1.setExpired();
        cache.release(proxyUGI1, false);
        UGICache.Entry proxyUGI2 = cache.getTimedProxyUGI(session);
        assertEquals(proxyUGI2, proxyUGI1);
        verify(provider, never()).destroy(any(UserGroupInformation.class));
    }

    @Test
    public void releaseDoesNotFreeResourcesIfUGIIsUsedElsewhere() throws Exception {
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);
        UGICache.Entry proxyUGI2 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, true);
        // UGI was not cleaned up because proxyUGI2 is referencing it
        UGICache.Entry proxyUGI3 = cache.getTimedProxyUGI(session);
        assertEquals(proxyUGI1, proxyUGI2);
        assertEquals(proxyUGI1, proxyUGI3);
        verify(provider, times(1)).createProxyUGI("the-user");
    }

    @Test
    public void releasingAllReferencesFreesResources() throws Exception {
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);
        UGICache.Entry proxyUGI2 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, true);
        cache.release(proxyUGI2, true);
        // at this point, the initial UGI has been freed. Calling
        // getTimedProxyUGI again creates a new UGI.
        UGICache.Entry proxyUGI3 = cache.getTimedProxyUGI(session);
        assertEquals(proxyUGI1, proxyUGI2);
        assertNotEquals(proxyUGI1, proxyUGI3);
        verify(provider, times(2)).createProxyUGI("the-user");
    }

    @Test
    public void releaseAnEntryNotInTheCache() throws Exception {
        // this could happen if some caller of the cache
        // retains a reference to a cache entry after releasing it.
        UGICache.Entry proxyUGI1 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, true);
        cache.release(proxyUGI1, true);
        UGICache.Entry proxyUGI2 = cache.getTimedProxyUGI(session);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(2)).createProxyUGI("the-user");
    }
}
