package com.pivotal.pxf.utilities;


/*
 * Base class for all plugin types (Accessor, Resolver, Fragmenter, Analyzer, ...)
 * Manages the meta data
 */
public class Plugin
{
	protected InputData inputData;
	
	/*
	 * C'tor
	 */
	public Plugin(InputData input)
	{
		this.inputData = input;
	}
	
}
