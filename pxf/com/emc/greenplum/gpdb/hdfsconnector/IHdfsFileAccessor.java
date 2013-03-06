package com.emc.greenplum.gpdb.hdfsconnector;

/*
 * Internal interface that defines the access to a file on HDFS.  All classes
 * that implement actual access to an HDFS file (sequence file, avro file,...)
 * must respect this interface
 */
public interface IHdfsFileAccessor
{
	boolean Open() throws Exception;
	OneRow LoadNextObject() throws Exception;
	void Close() throws Exception;
}
