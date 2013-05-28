package com.pivotal.pxf.utilities;


/*
 * Base class for all plugin types (Accessor, Resolver, Fragmenter, Analyzer, ...)
 * Manages the meta data
 */
public class Plugin
{
	private BaseMetaData metaData;
	
	/*
	 * C'tor
	 */
	public Plugin(BaseMetaData metaData)
	{
		this.metaData = metaData;
	}
	
	/*
	 * Access Method
	 */
	protected BaseMetaData getMetaData()
	{
		return metaData;
	}
}