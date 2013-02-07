package com.emc.greenplum.gpdb.hdfsconnector;

import com.emc.greenplum.gpdb.hadoop.io.GPDBWritable;
import org.apache.hadoop.io.Writable;

/*
 * IBridge interface - defines the interface of the Bridge classes.
 * Any Bridge class performs the task of loading an HDFS file (containing
 * records in some user-chosen serialization method), into an iteratable sequence
 * of GPDBWritable objects that can be sent to the postgres backend to be mapped
 * to a GPDB tuple.
 * There are many Bridge classes, since each class handles only one user file type
 * and serialization method. (file type examples: regular file, sequence file, avro file,...)
 * (serialization method examples: Writable, Google PB, Avro, Thrift)
 * Nevertheless, all Bridge classes must handle the GPDBWritable objects sequence  
 * to the client code in the same manner, and this is defined by the IBridge interface
 */
public interface IBridge 
{
	boolean BeginIteration() throws Exception;
	Writable GetNext() throws Exception;
}
