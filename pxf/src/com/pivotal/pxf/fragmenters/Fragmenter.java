package com.pivotal.pxf.fragmenters;

import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

/*
 * Interface that defines the splitting of a data resource into fragments that can
 * be processed in parallel
 * * GetFragments returns the fragments information of a given path (source name and location of each fragment).
 * Used to get fragments of data that could be read in parallel from the different segments.
 */
public abstract class Fragmenter extends Plugin
{
	/**
	 * Delimiter between fragment location elements
	 */
	public static final String FRAGMENTER_LOCATION_DELIM = "!FLD!";
	
	protected FragmentsOutput fragments;
	
	public Fragmenter(InputData metaData)
	{
		super(metaData);
		fragments = new FragmentsOutput();
	}
	
	/*
	 * path is a data source URI that can appear as a file name, a directory name  or a wildcard
	 * returns the data fragments
	 */
	public abstract FragmentsOutput GetFragments() throws Exception;
	
}
