package com.emc.greenplum.gpdb.hdfsconnector;

import org.apache.hadoop.fs.Path;

/*
 * Interface that defines the splitting of a data resource into fragments that can
 * be processed in parallel
 * * GetFragments returns the fragments information of a given path (source name and location of each fragment).
 * Used to get fragments of data that could be read in parallel from the different segments.
 * * GetStats returns statistics for a given path (block size, number of blocks, number of tuples).
 * Used when calling ANALYZE on a GPXF external table, to get table's statistics that are used by the optimizer to plan queries. 
 */
public interface IDataFragmenter
{
	/*
	 * path is a data source URI that can appear as a file name, a directory name  or a wildcard
	 * returns the data fragments in json format
	 */
	public String GetFragments(String data) throws Exception;
	
	/*
	 * path is a data source URI that can appear as a file name, a directory name  or a wildcard
	 * returns the data statistics in json format
	 */
	public String GetStats(String data) throws Exception;
}