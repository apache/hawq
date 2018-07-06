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
    //private static DelayQueue<TimedProxyUGI> delayQueue = new DelayQueue<>();
    private static DelayQueue<TimedProxyUGI>[] delayQueues = new DelayQueue<>[64];
    public static long UGI_CACHE_EXPIRY = 15 * 1 * 1000L; // 15 Minutes

    public UGICache() {
        for (int i = 0; i < delayQueues.length; i++) {
            delayQueues[i] = new DelayQueue<>();
        }
    }

    public TimedProxyUGI getTimedProxyUGI(String user, SegmentTransactionId session) throws IOException {

        Integer segmentId = session.getSegmentId();
        synchronized (delayQueues[segmentId]) {
            // use the opportunity to cleanup any expired entries
            cleanup(segmentId);
            TimedProxyUGI timedProxyUGI = cache.get(session);
            if (timedProxyUGI == null) {
                LOG.info(session.toString() + " Creating proxy user = " + user);
                UserGroupInformation proxyUGI =
                        UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
                timedProxyUGI = new TimedProxyUGI(proxyUGI, session);
                delayQueues[segmentId].offer(timedProxyUGI);
                cache.put(session, timedProxyUGI);
            }
            timedProxyUGI.incrementCounter();
            return timedProxyUGI;
        }
    }

    private cleanup(Integer segmentId) {
        // poll segment expiration queue for all expired entries and clean them if possible   
        TimedProxyUGI ugi = null;
        while ((ugi = delayQueues[segmentId].poll()) != null) {
            // place it back in the queue if still in use and was not closed
            if (!closeUGI(ugi)) {
                delayQueues[segmentId].offer(ugi);
            }
            LOG.info("Delay Queue Size for segment " +
                    segmentId + " = " + delayQueues[segmentId].size());
        }
    }
    
    public void release(TimedProxyUGI timedProxyUGI, boolean forceClean) {

        Integer segmentId = timedProxyUGI.getSession().getSegmentId();
        
        timedProxyUGI.resetTime();
        timedProxyUGI.decrementCounter();
        
        if (forceClean) {
            synchronized (delayQueues[segmentId]) {
                timedProxyUGI.setExpired();
                closeUGI(timedProxyUGI);
            }
        }
    }

    private static boolean closeUGI(TimedProxyUGI expiredProxyUGI) {

        SegmentTransactionId session = expiredProxyUGI.getSession();
        Integer segmentId = session.getSegmentId();
        //synchronized (delayQueues[segmentId]) {
            String fsMsg = "FileSystem for proxy user = " + expiredProxyUGI.getProxyUGI().getUserName();
            try {
                // The UGI object is still being used by another thread
                if (expiredProxyUGI.inProgress.get() != 0) {
                    LOG.info(session.toString() + " Skipping close of " + fsMsg);
                    expiredProxyUGI.resetTime(); // update time so that it doesn't expire until release updates time again
                    return false;
                }
                // expired UGI can be cleaned, as it is not used
                // determine if it can be removed from cache also
                TimedProxyUGI cachedUGI = cache.get(session);

                if (expiredProxyUGI == cachedUGI) {
                    // remove it from cache, as cache now has expired entry which is not in progress
                    cache.remove(session);
                }
                
                if (!expiredProxyUGI.isCleaned()) {
                    LOG.info(session.toString() + " Closing " + fsMsg +
                            " (Cache Size = " + cache.size() + ")");
                    FileSystem.closeAllForUGI(expiredProxyUGI.getProxyUGI());
                    expiredProxyUGI.setCleaned();
                }
                
            } catch (Throwable t) {
                LOG.warn(session.toString() + " Error closing " + fsMsg);
            }
        //}
    }

}
