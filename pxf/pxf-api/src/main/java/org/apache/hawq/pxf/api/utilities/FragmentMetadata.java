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

package org.apache.hawq.pxf.api.utilities;

/**
 * Class which holds metadata of a file split and locality information.
 *
 */
public class FragmentMetadata {

    private long start;
    private long end;
    private String[] hosts;

    public FragmentMetadata(long start, long end, String[] hosts) {
        this.start = start;
        this.end = end;
        this.hosts = hosts;
    }

    /**
     * Returns start position of a fragment
     * @return position in bytes where given data fragment starts
     */
    public long getStart() {
        return start;
    }

    /**
     * Sets start position of a fragment
     * @param start start position
     */
    public void setStart(long start) {
        this.start = start;
    }

    /**
     * Returns end positoon of a fragment
     * @return position in bytes where given data fragment ends
     */
    public long getEnd() {
        return end;
    }

    /**
     * Sets end position of a fragment
     * @param end end position
     */
    public void setEnd(long end) {
        this.end = end;
    }

    /**
     * Returns all hosts which have given data fragment
     * @return all hosts which have given data fragment
     */
    public String[] getHosts() {
        return hosts;
    }

    /**
     * Sets hosts for a given fragment
     * @param hosts hosts which have given fragment
     */
    public void setHosts(String[] hosts) {
        this.hosts = hosts;
    }

}
