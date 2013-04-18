package com.emc.greenplum.gpdb.hdfsconnector;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

/*
 * Specialization of SplittableFileAccessor for sequence files
 */
class SequenceFileAccessor extends SplittableFileAccessor
{
	/*
	 * C'tor
	 * Creates the InputFormat and the RecordReader object
	 */	
	public SequenceFileAccessor(HDFSMetaData meta) throws Exception
	{
		super(meta,
			  new SequenceFileInputFormat<LongWritable, Writable>());
 	}
	
	/*
	 * Override virtual method to create specialized record reader
	 */
	protected Object getReader(JobConf jobConf, InputSplit split) throws IOException
	{
		return new SequenceFileRecordReader(jobConf, (FileSplit)split);
	}
}
