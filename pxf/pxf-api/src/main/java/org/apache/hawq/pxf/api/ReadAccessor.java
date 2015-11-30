package org.apache.hawq.pxf.api;

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
 * Interface that defines access to the source data store (e.g, a file on HDFS, a region of an HBase table, etc).
 */
public interface ReadAccessor {
    /**
     * Opens the resource for reading.
     *
     * @return true if the resource is successfully opened
     * @throws Exception if opening the resource failed
     */
    boolean openForRead() throws Exception;

    /**
     * Reads the next object.
     *
     * @return the object which was read
     * @throws Exception if reading from the resource failed
     */
    OneRow readNextObject() throws Exception;

    /**
     * Closes the resource.
     *
     * @throws Exception if closing the resource failed
     */
    void closeForRead() throws Exception;
}
