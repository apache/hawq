package com.emc.greenplum.gpdb.hdfsconnector;

import com.emc.greenplum.gpdb.hadoop.io.GPDBWritable;
import java.io.DataOutputStream;
import java.lang.ClassNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import org.apache.hadoop.io.Writable;

/*
 * The Bridge class is a mediator between a given user data file in HDFS and 
 * the GPDB HDFS input file - which is always a sequence file where the key is 
 * the segment ID and the value is a GPDBWritable object.
 * Since the gpdbhdfsprotocol used by the GPDB external table to read an HDFS file expects
 * the hdfs file to be a sequence_file<segment_id, GPDBWritable>, and a given User 
 * data file will often be in some different format, we need the Bridge class to create
 * a GPDB input file based on the original user file.
 */
public class GPHdfsBridge extends BasicBridge
{	
	/*
	 * C'tor
	 */
	public GPHdfsBridge(HDFSMetaData metaData) throws Exception
	{
		super(metaData, getFileAccessor(metaData), getFieldsResolver(metaData));
	}

	/*
	 * Creates the file accessor based on the input file type
	 */
	static public IHdfsFileAccessor getFileAccessor(HDFSMetaData metaData) throws Exception
	{
		IHdfsFileAccessor accessor = null;
		String className = (metaData.outputFormat() == OutputFormat.FORMAT_TEXT) ?
				"TextFileAccessor" : metaData.accessor();

		return (IHdfsFileAccessor)Utilities.createAnyInstance(HDFSMetaData.class, className, metaData);
	}

	/*
	 * Creates the record fields resolver based on the input serialization method
	 */	
	static IFieldsResolver getFieldsResolver(HDFSMetaData metaData) throws Exception
	{
		IFieldsResolver resolver = null;
		String className = (metaData.outputFormat() == OutputFormat.FORMAT_TEXT) ?
				"TextResolver" : metaData.resolver();

		return (IFieldsResolver)Utilities.createAnyInstance(HDFSMetaData.class, className, metaData);
	}

}
