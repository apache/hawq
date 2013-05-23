package com.pivotal.pxf.analyzers;

import com.pivotal.pxf.utilities.BaseMetaData;
import com.pivotal.pxf.utilities.Utilities;

/*
 * Factory class for creation of Analyzer objects. The actual Analyzer object is "hidden" behind
 * an IAnalyzer interface which is returned by the AnalyzerFactory. 
 */
public class AnalyzerFactory
{
	static public IAnalyzer create(BaseMetaData conf) throws Exception
	{
		String analyzerName = conf.getProperty("X-GP-ANALYZER");
		return (IAnalyzer)Utilities.createAnyInstance(BaseMetaData.class, analyzerName, "analyzers", conf);
	}
}
