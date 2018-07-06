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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.security.UserGroupInformation;

public class TimedProxyUGI implements Delayed {

    private volatile long startTime;
    private UserGroupInformation proxyUGI;
    private SegmentTransactionId session;
    private boolean cleaned = false;
    AtomicInteger inProgress = new AtomicInteger();

    public TimedProxyUGI(UserGroupInformation proxyUGI, SegmentTransactionId session) {
        this.startTime = System.currentTimeMillis();
        this.proxyUGI = proxyUGI;
        this.session = session;
    }

    public UserGroupInformation getProxyUGI() {
        return proxyUGI;
    }

    public SegmentTransactionId getSession() {
        return session;
    }

    public boolean isCleaned() {
        return cleaned;
    }

    public void setCleaned() {
        cleaned = true;
    }

    public int getCounter() {
        return inProgress.get();
    }

    public void incrementCounter() {
        inProgress.incrementAndGet();
    }

    public void decrementCounter() {
        inProgress.decrementAndGet();
    }

    public void resetTime() {
        startTime = System.currentTimeMillis();
    }

    public void setExpired() {
        startTime = -1L;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(getDelayMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        TimedProxyUGI that = (TimedProxyUGI) other;
        return Long.compare(this.getDelayMillis(), that.getDelayMillis());
    }

    public long getDelayMillis() {
        return (startTime + UGICache.UGI_CACHE_EXPIRY) - System.currentTimeMillis();
    }
}

