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

/**
 * For the purposes of pxf-server, a session is the set of requests processed on a specific segment
 * on behalf of a particular user and transaction. Grouping requests together into a session allows
 * us to re-use the UserGroupInformation object (which is expensive to destroy) for each session.
 * <p>
 * SessionId is used as the cache key to look up the UserGroupInformation for a request. See {@link
 * UGICache}.
 */
public class SessionId {

    private final String user;
    private final Integer segmentId;
    private final String sessionId;

    /**
     * Create a sessionId
     *
     * @param segmentId     the calling segment
     * @param transactionId the identifier for the transaction
     * @param gpdbUser      the GPDB username
     */
    public SessionId(Integer segmentId, String transactionId, String gpdbUser) {
        this.segmentId = segmentId;
        this.user = gpdbUser;
        this.sessionId = gpdbUser + ":" + transactionId + ":" + segmentId;
    }

    /**
     * @return the segment id
     */
    public Integer getSegmentId() {
        return segmentId;
    }

    /**
     * @return the gpdb user name
     */
    public String getUser() {
        return user;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return sessionId.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (obj.getClass() != getClass()) return false;

        SessionId that = (SessionId) obj;
        return this.sessionId.equals(that.sessionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Session = " + sessionId;
    }
}
