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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

public class UGICache {

    private static final Log LOG = LogFactory.getLog(UGICache.class);
    private static Map<SegmentTransactionId, TimedProxyUGI> cache = new ConcurrentHashMap<>();
    private static DelayQueue<TimedProxyUGI> delayQueue = new DelayQueue<>();
    private static Object[] segmentLocks = new Object[100];
    public static long UGI_CACHE_EXPIRY = 15 * 1 * 1000L; // 15 Minutes

    static {
        Thread t = new Thread(new Runnable() {

            public void run() {
                while (true) {
                    try {
                        Thread.sleep(UGI_CACHE_EXPIRY);
                        TimedProxyUGI timedProxyUGI = delayQueue.poll();
                        while (timedProxyUGI != null) {
                            closeUGI(timedProxyUGI);
                            LOG.info("Delay Queue Size = " + delayQueue.size());
                            timedProxyUGI = delayQueue.poll();
                        }
                    } catch (InterruptedException ie) {
                        LOG.warn("Terminating reaper thread");
                        return;
                    }
                }
            }
        });

        t.setName("UGI Reaper Thread");
        t.start();
        for (int i = 0; i < segmentLocks.length; i++) {
            segmentLocks[i] = new Object();
        }
    }

    public TimedProxyUGI getTimedProxyUGI(String user, SegmentTransactionId session) throws IOException {

        Integer segmentId = session.getSegmentId();
        synchronized (segmentLocks[segmentId]) {
            TimedProxyUGI timedProxyUGI = cache.get(session);
            if (timedProxyUGI == null) {
                LOG.info(session.toString() + " Creating proxy user = " + user);
                UserGroupInformation proxyUGI =
                        UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
                timedProxyUGI = new TimedProxyUGI(proxyUGI, session);
                delayQueue.offer(timedProxyUGI);
                cache.put(session, timedProxyUGI);
            } else if (timedProxyUGI.getDelayMillis() < 0) {
                closeUGI(timedProxyUGI);
            } else {
                timedProxyUGI.incrementCounter();
            }
            return timedProxyUGI;
        }
    }

    public void release(TimedProxyUGI timedProxyUGI, Integer fragmentIndex, Integer fragmentCount) {

        Integer segmentId = timedProxyUGI.getSession().getSegmentId();
        synchronized (segmentLocks[segmentId]) {
            timedProxyUGI.resetTime();
            timedProxyUGI.decrementCounter();
            if (fragmentIndex != null && fragmentCount.equals(fragmentIndex))
                closeUGI(timedProxyUGI);
        }
    }

    private static void closeUGI(TimedProxyUGI timedProxyUGI) {

        Integer segmentId = timedProxyUGI.getSession().getSegmentId();
        synchronized (segmentLocks[segmentId]) {
            String fsMsg = "FileSystem for proxy user = " + timedProxyUGI.getProxyUGI().getUserName();
            try {
                if (timedProxyUGI.inProgress.get() != 0) {
                    LOG.info(timedProxyUGI.getSession().toString() + " Skipping close of " + fsMsg);
                    timedProxyUGI.resetTime();
                    delayQueue.offer(timedProxyUGI);
                    return;
                }
                if (cache.remove(timedProxyUGI.getSession()) != null) {
                    LOG.info(timedProxyUGI.getSession().toString() + " Closing " + fsMsg +
                            " (Cache Size = " + cache.size() + ")");
                    FileSystem.closeAllForUGI(timedProxyUGI.getProxyUGI());
                }
            } catch (Throwable t) {
                LOG.warn(timedProxyUGI.getSession().toString() + " Error closing " + fsMsg);
            }
        }
    }

}