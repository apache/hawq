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

public class SessionId {

    private final String user;
    private Integer segmentId;
    private String sessionId;

    public SessionId(Integer segmentId, String transactionId, String gpdbUser) {
        this.segmentId = segmentId;
        this.user = gpdbUser;
        this.sessionId = segmentId + ":" + transactionId + ":" + gpdbUser;
    }

    public Integer getSegmentId() {
        return segmentId;
    }

    @Override
    public int hashCode() {
        return sessionId.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SessionId)) {
            return false;
        }
        SessionId that = (SessionId) other;
        return this.sessionId.equals(that.sessionId);
    }

    @Override
    public String toString() {
        return "Session = " + sessionId;
    }

    public String getUser() {
        return user;
    }
}
