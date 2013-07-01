package com.pivotal.pxf.resolvers;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import com.pivotal.pxf.resolvers.StringPassResolver;
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
