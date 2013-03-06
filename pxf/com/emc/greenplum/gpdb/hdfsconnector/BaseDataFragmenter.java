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
	
	/*
	 * (non-Javadoc)
	 * @see com.emc.greenplum.gpdb.hdfsconnector.IDataFragmenter#GetStats(java.lang.String)
	 */
	public String GetStats(String data) throws Exception
	{
		/*
		 * return default values
		 */
		return DataSourceStatsInfo.dataToJSON(new DataSourceStatsInfo());
	}
}