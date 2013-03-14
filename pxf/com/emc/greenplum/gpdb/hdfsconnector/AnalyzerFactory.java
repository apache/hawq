package com.emc.greenplum.gpdb.hdfsconnector;

/*
 * Factory class for creation of Analyzer objects. The actual Analyzer object is "hidden" behind
 * an IAnalyzer interface which is returned by the AnalyzerFactory. 
 */
public class AnalyzerFactory
{
	static public IAnalyzer create(BaseMetaData conf) throws Exception
	{
		String analyzerName = conf.getProperty("X-GP-ANALYZER");
		return (IAnalyzer)Utilities.createAnyInstance(BaseMetaData.class, analyzerName, conf);
	}
}
