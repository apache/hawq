package com.pivotal.pxf.plugins.hdfs;

import com.pivotal.pxf.api.utilities.InputData;

/*
 * @deprecated - use LineBreakAccessor
 */
@Deprecated
public class LineReaderAccessor extends LineBreakAccessor
{
	/*
	 * C'tor
	 * Creates the TextFileAccessor
	 */
	public LineReaderAccessor(InputData input) throws Exception
	{
		super(input);
 	}
}
