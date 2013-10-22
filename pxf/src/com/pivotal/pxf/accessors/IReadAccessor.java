package com.pivotal.pxf.accessors;

import com.pivotal.pxf.format.OneRow;

/*
 * Internal interface that defines the access to data on the source
 * data store (e.g, a file on HDFS, a region of an HBase table, etc).
 * All classes that implement actual access to such data sources must 
 * respect this interface
 */
public interface IReadAccessor 
{
	public boolean openForRead() throws Exception;
	public OneRow readNextObject() throws Exception;
	public void closeForRead() throws Exception;
}
