package com.pivotal.pxf.fragmenters;

/*
 * Interface that defines the splitting of a data resource into fragments that can
 * be processed in parallel
 * * GetFragments returns the fragments information of a given path (source name and location of each fragment).
 * Used to get fragments of data that could be read in parallel from the different segments.
 */
public interface IDataFragmenter
{
	/*
	 * path is a data source URI that can appear as a file name, a directory name  or a wildcard
	 * returns the data fragments in json format
	 */
	public String GetFragments(String data) throws Exception;
	
}