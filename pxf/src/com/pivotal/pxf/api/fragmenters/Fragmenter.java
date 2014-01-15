package com.pivotal.pxf.api.fragmenters;

import com.pivotal.pxf.api.utilities.Plugin;
import com.pivotal.pxf.api.utilities.InputData;

import java.util.LinkedList;
import java.util.List;

/*
 * Interface that defines the splitting of a data resource into fragments that can
 * be processed in parallel
 * getFragments() returns the fragments information of a given path (source name and location of each fragment).
 * Used to get fragments of data that could be read in parallel from the different segments.
 */
public abstract class Fragmenter extends Plugin
{
	protected List<Fragment> fragments;
	
	public Fragmenter(InputData metaData)
	{
		super(metaData);
		fragments = new LinkedList<Fragment>();
	}
	
	/*
	 * path is a data source URI that can appear as a file name, a directory name  or a wildcard
	 * returns the data fragments
	 */
	public abstract List<Fragment> getFragments() throws Exception;
}
