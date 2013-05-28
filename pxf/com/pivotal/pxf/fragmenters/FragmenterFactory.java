package com.pivotal.pxf.fragmenters;

import com.pivotal.pxf.utilities.BaseMetaData;
import com.pivotal.pxf.utilities.Utilities;

/*
 * Factory class for creation of Fragmenter objects. The actual Fragmenter object is "hidden" behind
 * an Fragmenter abstract class which is returned by the FragmenterFactory. 
 */
public class FragmenterFactory
{
	static public Fragmenter create(BaseMetaData conf) throws Exception
	{
		String fragmenterName = conf.getProperty("X-GP-FRAGMENTER");
		return (Fragmenter)Utilities.createAnyInstance(BaseMetaData.class, fragmenterName, "fragmenters", conf);
	}
}
