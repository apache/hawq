package com.pivotal.pxf.accessors;

import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

/*
 * Internal interface that defines the access to a file on HDFS.  All classes
 * that implement actual access to an HDFS file (sequence file, avro file,...)
 * must respect this interface
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
