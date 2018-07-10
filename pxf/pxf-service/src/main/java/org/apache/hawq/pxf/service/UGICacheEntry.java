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

public class UGICacheEntry implements Delayed {

    private volatile long startTime;
    private UserGroupInformation proxyUGI;
    private SessionId session;
    private boolean cleaned = false;
    private AtomicInteger inProgress = new AtomicInteger();
    private static long UGI_CACHE_EXPIRY = 15 * 60 * 1000L; // 15 Minutes

    UGICacheEntry(UserGroupInformation proxyUGI, SessionId session) {
        this.startTime = System.currentTimeMillis();
        this.proxyUGI = proxyUGI;
        this.session = session;
    }

    public UserGroupInformation getUGI() {
        return proxyUGI;
    }

    SessionId getSession() {
        return session;
    }

    boolean isCleaned() {
        return cleaned;
    }

    void setCleaned() {
        cleaned = true;
    }

    int getCounter() {
        return inProgress.get();
    }

    void incrementCounter() {
        inProgress.incrementAndGet();
    }

    void decrementCounter() {
        inProgress.decrementAndGet();
    }

    void resetTime() {
        startTime = System.currentTimeMillis();
    }

    void setExpired() {
        startTime = -1L;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(getDelayMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        UGICacheEntry that = (UGICacheEntry) other;
        return Long.compare(this.getDelayMillis(), that.getDelayMillis());
    }

    private long getDelayMillis() {
        return (startTime + UGI_CACHE_EXPIRY) - System.currentTimeMillis();
    }
}

