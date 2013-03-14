package com.emc.greenplum.gpdb.hdfsconnector;


/*
 * Base class for the analyzer
 */
public abstract class BaseAnalyzer implements IAnalyzer
{
	protected BaseMetaData conf;
	
	/*
	 * C'tor
	 */
	public BaseAnalyzer(BaseMetaData inConf)
	{
		conf = inConf;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.emc.greenplum.gpdb.hdfsconnector.IAnalyzer#GetEstimatedStats(java.lang.String)
	 */
	public String GetEstimatedStats(String data) throws Exception
	{
		/*
		 * return default values
		 */
		return DataSourceStatsInfo.dataToJSON(new DataSourceStatsInfo());
	}
}