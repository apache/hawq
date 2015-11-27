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


import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;

/**
 * Abstract class that defines getting statistics for ANALYZE.
 * {@link #getEstimatedStats} returns statistics for a given path
 * (block size, number of blocks, number of tuples).
 * Used when calling ANALYZE on a PXF external table, to get
 * table's statistics that are used by the optimizer to plan queries.
 */
public abstract class Analyzer extends Plugin {
    /**
     * Constructs an Analyzer.
     *
     * @param inputData the input data
     */
    public Analyzer(InputData inputData) {
        super(inputData);
    }

    /**
     * Gets the statistics for a given path.
     * NOTE: It is highly recommended to implement an extremely fast logic
     * that returns *estimated* statistics. Scanning all the data for exact
     * statistics is considered bad practice.
     *
     * @param data the data source name (e.g, file, dir, wildcard, table name).
     * @return AnalyzerStats the data statistics in json format.
     * @throws Exception if fails to get stats
     */
    public AnalyzerStats getEstimatedStats(String data) throws Exception {
        /* Return default values */
        return new AnalyzerStats();
    }
}
