package com.pivotal.pxf.analyzers;

import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

/*
 * Abstract class that defines getting statistics for ANALYZE.
 * GetEstimatedStats returns statistics for a given path 
 * (block size, number of blocks, number of tuples).
 * Used when calling ANALYZE on a GPXF external table, 
 * to get table's statistics that are used by the optimizer to plan queries. 
 */
public abstract class Analyzer extends Plugin
{
	public Analyzer(InputData metaData)
	{
		super(metaData);
	}
	
	/*
	 * path is a data source URI that can appear as a file name, a directory name  or a wildcard
	 * returns the data statistics in json format
	 */
	public String GetEstimatedStats(String data) throws Exception
	{
		/*
		 * return default values
		 */
		return DataSourceStatsInfo.dataToJSON(new DataSourceStatsInfo());
	}	
}
