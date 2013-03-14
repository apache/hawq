package com.emc.greenplum.gpdb.hdfsconnector;


/*
 * Interface that defines getting statistics for ANALYZE.
 * GetEstimatedStats returns statistics for a given path 
 * (block size, number of blocks, number of tuples).
 * Used when calling ANALYZE on a GPXF external table, 
 * to get table's statistics that are used by the optimizer to plan queries. 
 */
public interface IAnalyzer
{
	
	/*
	 * path is a data source URI that can appear as a file name, a directory name  or a wildcard
	 * returns the data statistics in json format
	 */
	public String GetEstimatedStats(String data) throws Exception;
}