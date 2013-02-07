package com.emc.greenplum.gpdb.hdfsconnector;

import org.apache.hadoop.fs.Path;

/*
 * Interface that defines the splitting of a data resource into fragments that can
 * be processed in paralel
 */
public interface IDataFragmenter
{
	/*
	 * path is a data source URI that can appear as a file name, a directory name  or a wildcard
	 * returns the data fragments in json format
	 */
	public String GetFragments(String data) throws Exception;
}