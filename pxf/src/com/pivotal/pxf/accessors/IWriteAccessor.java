package com.pivotal.pxf.accessors;

import com.pivotal.pxf.format.OneRow;

/*
 * An interface for writing data into a data store 
 * (e.g. a sequence file on HDFS).
 * All classes that implement actual access to such data sources must 
 * implement this interface.
 */
public interface IWriteAccessor 
{
	
	// opens file for write
	public boolean openForWrite() throws Exception;
	public boolean writeNextObject(OneRow onerow) throws Exception;
	public void closeForWrite() throws Exception;
}
