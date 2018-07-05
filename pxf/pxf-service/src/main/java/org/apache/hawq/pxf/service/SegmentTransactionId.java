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

public class SegmentTransactionId {

    private Integer segmentId;
    private String transactionId;
    private String segmentTransactionId;

    public SegmentTransactionId(Integer segmentId, String transactionId) {
        this.segmentId = segmentId;
        this.transactionId = transactionId;
        this.segmentTransactionId = segmentId + ":" + transactionId;
    }

    public Integer getSegmentId() {
        return segmentId;
    }

    public String getSegmentTransactionId() {
        return segmentTransactionId;
    }

    @Override
    public int hashCode() {
        return segmentTransactionId.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SegmentTransactionId)) {
            return false;
        }
        SegmentTransactionId that = (SegmentTransactionId) other;
        return this.segmentTransactionId.equals(that.segmentTransactionId);
    }

    @Override
    public String toString() {
        return "Session = " + segmentTransactionId;
    }
}