package com.pivotal.pxf.plugins.hdfs;

import com.pivotal.pxf.api.utilities.InputData;

/*
 * @deprecated - use StringPassResolver
 */
@Deprecated
public class TextResolver extends StringPassResolver
{
	/*
	 * C'tor
	 */
	public TextResolver(InputData input) throws Exception
	{
		super(input);
	}
}
