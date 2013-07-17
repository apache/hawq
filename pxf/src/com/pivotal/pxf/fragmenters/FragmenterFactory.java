package com.pivotal.pxf.fragmenters;

import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Utilities;

/*
 * Factory class for creation of Fragmenter objects. The actual Fragmenter object is "hidden" behind
 * an Fragmenter abstract class which is returned by the FragmenterFactory. 
 */
public class FragmenterFactory
{
	static public Fragmenter create(InputData conf) throws Exception
	{
		String fragmenterName = conf.getProperty("X-GP-FRAGMENTER");
		return (Fragmenter)Utilities.createAnyInstance(InputData.class, fragmenterName, "fragmenters", conf);
	}
}
