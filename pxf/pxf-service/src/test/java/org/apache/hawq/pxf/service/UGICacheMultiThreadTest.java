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

import io.netty.util.internal.ConcurrentSet;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class UGICacheMultiThreadTest {
    private FakeUgiProvider provider = null;
    private static final int numberOfSegments = 3;
    private static final int numberOfUsers = 3;
    private static final int numberOfTxns = 3;
    private SessionId[] sessions = new SessionId[numberOfSegments * numberOfUsers * numberOfTxns];
    private UGICache cache = null;
    private UGICacheTest.FakeTicker fakeTicker;

    class FakeUgiProvider extends UGIProvider {
        Set<UserGroupInformation> ugis = new ConcurrentSet<>();

        @Override
        UserGroupInformation createProxyUGI(String effectiveUser) {
            UserGroupInformation ugi = mock(UserGroupInformation.class);
            ugis.add(ugi);
            return ugi;
        }

        @Override
        void destroy(UserGroupInformation ugi) {
            if (!ugis.remove(ugi)) {
                throw new IllegalStateException("Tried to destroy UGI that does not exist");
            }
        }

        int countUgisInUse() {
            return ugis.size();
        }
    }

    @Before
    public void setUp() {
        provider = new FakeUgiProvider();

        int l = 0;
        for (int i = 0; i < numberOfSegments; i++) {
            for (int j = 0; j < numberOfUsers; j++) {
                for (int k = 0; k < numberOfTxns; k++) {
                    sessions[l++] = new SessionId(i, "txn-id-" + k, "the-user-" + j);
                }
            }
        }
        fakeTicker = new UGICacheTest.FakeTicker();
        cache = new UGICache(provider, fakeTicker);
    }

    @Test
    public void multiThreadedTest() throws Exception {
        final Random rnd = new SecureRandom();
        final AtomicInteger finishedCount = new AtomicInteger();

        int threadCount = 500;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < 100; i++) {
                            for (SessionId session : sessions) {
                                cache.getUserGroupInformation(session);
                            }
                            Thread.sleep(0, rnd.nextInt(1000));
                            for (SessionId session : sessions) {
                                cache.release(session, false);
                            }
                        }

                        for (SessionId session : sessions) {
                            cache.getUserGroupInformation(session);
                            cache.release(session, true);
                        }

                        finishedCount.incrementAndGet();
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(threadCount, finishedCount.intValue());
        assertEquals(0, provider.countUgisInUse());
        // after the test has completed, the internal cache
        // should be 0
        assertEquals(0, cache.size());
        assertEquals(0, cache.allQueuesSize());
    }
}
