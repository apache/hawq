package com.pivotal.pxf.accessors;

import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

/*
 * Internal interface that defines the access to data on the source
 * data store (e.g, a file on HDFS, a region of an HBase table, etc).
 * All classes that implement actual access such data sources must 
 * respect this interface
 */
public abstract class Accessor extends Plugin
{
	public Accessor(InputData metaData)
	{
		super(metaData);
	}
	
	abstract public boolean Open() throws Exception;
	abstract public OneRow LoadNextObject() throws Exception;
	abstract public void Close() throws Exception;
}
