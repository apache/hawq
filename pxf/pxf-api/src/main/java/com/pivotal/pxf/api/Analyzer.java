package com.pivotal.pxf.api;

import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

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
