package com.pivotal.pxf.bridge;

import com.pivotal.pxf.accessors.Accessor;
import com.pivotal.pxf.analyzers.AnalyzerFactory;
import com.pivotal.pxf.format.OutputFormat;
import com.pivotal.pxf.resolvers.Resolver;
import com.pivotal.pxf.utilities.HDFSMetaData;
import com.pivotal.pxf.utilities.Utilities;


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
	static public Accessor getFileAccessor(HDFSMetaData metaData) throws Exception
	{
		String className = (metaData.outputFormat() == OutputFormat.FORMAT_TEXT) ?
				"TextFileAccessor" : metaData.accessor();

		return (Accessor)Utilities.createAnyInstance(HDFSMetaData.class, className, "accessors", metaData);
	}

	/*
	 * Creates the record fields resolver based on the input serialization method
	 */	
	static Resolver getFieldsResolver(HDFSMetaData metaData) throws Exception
	{
		String className = (metaData.outputFormat() == OutputFormat.FORMAT_TEXT) ?
				"TextResolver" : metaData.resolver();

		return (Resolver)Utilities.createAnyInstance(HDFSMetaData.class, className, "resolvers", metaData);
	}

}
