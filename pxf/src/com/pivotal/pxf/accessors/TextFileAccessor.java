package com.pivotal.pxf.accessors;

import java.io.IOException;

import com.pivotal.pxf.accessors.LineReaderAccessor;
import com.pivotal.pxf.utilities.InputData;

/*
 * @deprecated - use LineReaderAccessor
 */
@Deprecated
public class TextFileAccessor extends LineReaderAccessor
{
	/*
	 * C'tor
	 * Creates the TextFileAccessor
	 */
	public TextFileAccessor(InputData input) throws Exception
	{
		super(input);
 	}
}
