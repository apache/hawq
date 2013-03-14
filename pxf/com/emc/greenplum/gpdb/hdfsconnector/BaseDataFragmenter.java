package com.emc.greenplum.gpdb.hdfsconnector;


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