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

package org.apache.hawq.pxf.api;

import org.apache.hawq.pxf.api.OneRow;

/**
 * Interface of accessor which can leverage statistic information for aggregate queries
 *
 */
public interface StatsAccessor extends ReadAccessor {

    /**
     * Method which reads needed statistics for current split
     * @throws Exception if retrieving the stats failed
     */
    public void retrieveStats() throws Exception;

    /**
     * Returns next tuple based on statistics information without actual reading of data
     * @return next row without reading it from disk
     */
    public OneRow emitAggObject();

}
