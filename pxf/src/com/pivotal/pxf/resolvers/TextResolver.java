package com.pivotal.pxf.resolvers;

import com.pivotal.pxf.utilities.InputData;

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
