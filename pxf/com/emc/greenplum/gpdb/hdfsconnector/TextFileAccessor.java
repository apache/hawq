package com.emc.greenplum.gpdb.hdfsconnector;

import java.io.IOException;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextInputFormat;	
import org.apache.hadoop.mapred.LineRecordReader;

/*
 * Specialization of SplittableFileAccessor for text files
 */
class TextFileAccessor extends SplittableFileAccessor
{
	/*
	 * C'tor
	 * Creates the TextInputFormat and the LineRecordReader object
	 */
	public TextFileAccessor(HDFSMetaData meta) throws Exception
	{
		super(meta,
			  new TextInputFormat());
		((TextInputFormat)fformat).configure(jobConf);
 	}
	
	/*
	 * Override virtual method to create specialized record reader
	 */	
	protected Object getReader(JobConf jobConf, InputSplit split) throws IOException
	{
		return new LineRecordReader(jobConf, (FileSplit)split);
	}
}
