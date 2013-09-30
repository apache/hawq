package com.pivotal.pxf.accessors;

import com.pivotal.pxf.accessors.LineBreakAccessor;
import com.pivotal.pxf.utilities.InputData;

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
