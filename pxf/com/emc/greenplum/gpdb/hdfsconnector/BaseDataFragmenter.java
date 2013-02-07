package com.emc.greenplum.gpdb.hdfsconnector;

import org.apache.hadoop.fs.Path;


/*
 * Base class for the data fragmenters
 */
public abstract class BaseDataFragmenter implements IDataFragmenter
{
	protected BaseMetaData conf;
	
	/*
	 * C'tor
	 */
	public BaseDataFragmenter(BaseMetaData inConf)
	{
		conf = inConf;
	}
}