package com.emc.greenplum.gpdb.hdfsconnector;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

/*
 * Specialization of SplittableFileAccessor for sequence files
 */
class HiveAccessor extends SplittableFileAccessor
{
	/*
	 * C'tor
	 * Creates the InputFormat and the RecordReader object
	 */	
	public HiveAccessor(HDFSMetaData meta) throws Exception
	{
		/*
		 * Unfortunately, Java does not allow us to call a function before calling the base constructor,
		 * otherwise it would have been:  super(meta, createInputFormat(meta))
		 */
		super(meta,
			  (FileInputFormat<?, ?>)null);
		fformat = createInputFormat(meta);
 	}
	
	/*
	 * Override virtual method to create specialized record reader
	 */
	protected Object getReader(JobConf jobConf, InputSplit split) throws IOException
	{
		return fformat.getRecordReader(split, jobConf, Reporter.NULL);
	}
	
	private FileInputFormat<?, ?> createInputFormat(HDFSMetaData meta) throws Exception
	{
		String userData = meta.getProperty("X-GP-FRAGMENT-USER-DATA");
		String[] toks = userData.split(HiveDataFragmenter.HIVE_USER_DATA_DELIM);
		return HiveDataFragmenter.makeInputFormat(toks[0]/* inputFormat name */, jobConf);
	}
}
