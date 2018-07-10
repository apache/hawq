package org.apache.hawq.pxf.service;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.when;

public class UGICacheTest {
    private UGIProvider provider = null;
    private SessionId session = null;
    private UGICache cache = null;

    @Before
    public void setUp() throws Exception {
        provider = mock(UGIProvider.class);

        when(provider.createProxyUGI(any(String.class))).thenAnswer((Answer<UserGroupInformation>) invocation -> mock(UserGroupInformation.class));

        session = new SessionId(0, "txn-id", "the-user");

        cache = new UGICache(provider);
    }

    @Test
    public void getUGIFromEmptyCache() throws Exception {
        UGICacheEntry entry = cache.getTimedProxyUGI(session);
        assertNotNull(entry.getUGI());
        verify(provider).createProxyUGI("the-user");
    }

    @Test
    public void getSameUGITwiceUsesCache() throws Exception {
        UGICacheEntry entry1 = cache.getTimedProxyUGI(session);
        UGICacheEntry entry2 = cache.getTimedProxyUGI(session);
        assertEquals(entry1, entry2);
        assertNotNull(entry1.getUGI());
        verify(provider, times(1)).createProxyUGI("the-user");
    }

    @Test
    public void getTwoUGIsWithDifferentSessionsForSameUser() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id-2", "the-user");
        UGICacheEntry proxyUGI1 = cache.getTimedProxyUGI(session);
        UGICacheEntry proxyUGI2 = cache.getTimedProxyUGI(otherSession);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(2)).createProxyUGI("the-user");
        // TODO: this seems weird. We're creating two UGIs with the same params,
        // even though we have two different sessions. Why?
    }

    @Test
    public void getTwoUGIsWithDifferentUsers() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id", "different-user");
        UGICacheEntry proxyUGI1 = cache.getTimedProxyUGI(session);
        UGICacheEntry proxyUGI2 = cache.getTimedProxyUGI(otherSession);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, times(1)).createProxyUGI("different-user");
    }

    @Test
    public void getTwoUGIsWithDifferentUsersCachesBoth() throws Exception {
        SessionId otherSession = new SessionId(0, "txn-id", "different-user");
        UGICacheEntry proxyUGI1a = cache.getTimedProxyUGI(session);
        UGICacheEntry proxyUGI1b = cache.getTimedProxyUGI(session);
        UGICacheEntry proxyUGI2a = cache.getTimedProxyUGI(otherSession);
        UGICacheEntry proxyUGI2b = cache.getTimedProxyUGI(otherSession);
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
    public void releaseWithoutForceClean() throws Exception {
        UGICacheEntry proxyUGI1 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, false);
        // UGI wasn't cleaned up, so we can still get it
        UGICacheEntry proxyUGI2 = cache.getTimedProxyUGI(session);
        assertEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(1)).createProxyUGI("the-user");
    }

    @Test
    public void releaseWithForceClean() throws Exception {
        UGICacheEntry proxyUGI1 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, true);
        UGICacheEntry proxyUGI2 = cache.getTimedProxyUGI(session);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(2)).createProxyUGI("the-user");
    }

    @Test
    public void releaseAndReacquireDoesNotFreeResources() throws Exception {
        UGICacheEntry proxyUGI1 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, false);
        UGICacheEntry proxyUGI2 = cache.getTimedProxyUGI(session);
        proxyUGI2.setExpired();
        UGICacheEntry proxyUGI3 = cache.getTimedProxyUGI(session);
        // this does not clean up any UGIs because our proxyUGI is still in use.
        assertEquals(proxyUGI3, proxyUGI2);
        verify(provider, times(1)).createProxyUGI("the-user");
        verify(provider, never()).destroy(any(UserGroupInformation.class));
    }

    @Test
    public void releaseAndAcquireAfterTimeoutFreesResources() throws Exception {
        UGICacheEntry proxyUGI1 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, false);
        proxyUGI1.setExpired();
        verify(provider, never()).destroy(any(UserGroupInformation.class));
        UGICacheEntry proxyUGI2 = cache.getTimedProxyUGI(session);
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
        UGICacheEntry proxyUGI1 = cache.getTimedProxyUGI(session);
        proxyUGI1.setExpired();
        cache.release(proxyUGI1, false);
        UGICacheEntry proxyUGI2 = cache.getTimedProxyUGI(session);
        assertEquals(proxyUGI2, proxyUGI1);
        verify(provider, never()).destroy(any(UserGroupInformation.class));
    }

    @Test
    public void releaseDoesNotFreeResourcesIfUGIIsUsedElsewhere() throws Exception {
        UGICacheEntry proxyUGI1 = cache.getTimedProxyUGI(session);
        UGICacheEntry proxyUGI2 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, true);
        // UGI was not cleaned up because proxyUGI2 is referencing it
        UGICacheEntry proxyUGI3 = cache.getTimedProxyUGI(session);
        assertEquals(proxyUGI1, proxyUGI2);
        assertEquals(proxyUGI1, proxyUGI3);
        verify(provider, times(1)).createProxyUGI("the-user");
    }

    @Test
    public void releasingAllReferencesFreesResources() throws Exception {
        UGICacheEntry proxyUGI1 = cache.getTimedProxyUGI(session);
        UGICacheEntry proxyUGI2 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, true);
        cache.release(proxyUGI2, true);
        // at this point, the initial UGI has been freed. Calling
        // getTimedProxyUGI again creates a new UGI.
        UGICacheEntry proxyUGI3 = cache.getTimedProxyUGI(session);
        assertEquals(proxyUGI1, proxyUGI2);
        assertNotEquals(proxyUGI1, proxyUGI3);
        verify(provider, times(2)).createProxyUGI("the-user");
    }

    @Test
    public void releaseAnEntryNotInTheCache() throws Exception {
        // this could happen if some caller of the cache
        // retains a reference to a cache entry after releasing it.
        UGICacheEntry proxyUGI1 = cache.getTimedProxyUGI(session);

        cache.release(proxyUGI1, true);
        cache.release(proxyUGI1, true);
        UGICacheEntry proxyUGI2 = cache.getTimedProxyUGI(session);
        assertNotEquals(proxyUGI1, proxyUGI2);
        verify(provider, times(2)).createProxyUGI("the-user");
    }
}
