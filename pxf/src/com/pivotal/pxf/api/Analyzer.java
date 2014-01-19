package com.pivotal.pxf.api;

import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

/*
 * Abstract class that defines getting statistics for ANALYZE.
 * getEstimatedStats returns statistics for a given path
 * (block size, number of blocks, number of tuples).
 * Used when calling ANALYZE on a PXF external table, to get 
 * table's statistics that are used by the optimizer to plan queries. 
 */
public abstract class Analyzer extends Plugin
{
	public Analyzer(InputData metaData)
	{
		super(metaData);
	}
	
	/*
	 * 'path' is the data source name (e.g, file, dir, wildcard, table name).
	 * returns the data statistics in json format.
	 * 
	 * NOTE: It is highly recommended to implement an extremely fast logic
	 * that returns *estimated* statistics. Scanning all the data for exact
	 * statistics is considered bad practice.
	 */
	public AnalyzerStats getEstimatedStats(String data, ReadAccessor accessor) throws Exception
	{
		/* Return default values */
		return new AnalyzerStats();
	}	
}
