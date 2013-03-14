package com.emc.greenplum.gpdb.hdfsconnector;

/*
 * Factory class for creation of Fragmenter objects. The actual Fragmenter object is "hidden" behind
 * an IDataFragmenter interface which is returned by the FragmenterFactory. 
 */
public class FragmenterFactory
{
	static public IDataFragmenter create(BaseMetaData conf) throws Exception
	{
		String fragmenterName = conf.getProperty("X-GP-FRAGMENTER");
		return (IDataFragmenter)Utilities.createAnyInstance(BaseMetaData.class, fragmenterName, conf);
	}
}
