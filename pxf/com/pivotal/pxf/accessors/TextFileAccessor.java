package com.pivotal.pxf.accessors;

import java.io.IOException;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;	
import org.apache.hadoop.mapred.LineRecordReader;

import com.pivotal.pxf.utilities.HDFSMetaData;

/*
 * Specialization of SplittableFileAccessor for text files
 */
public class TextFileAccessor extends SplittableFileAccessor
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
