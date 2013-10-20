package com.pivotal.pxf.accessors;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.utilities.InputData;

/*
 * Specialization of HdfsSplittableDataAccessor for sequence files
 */
public class SequenceFileAccessor extends HdfsSplittableDataAccessor
{

	private Log Log;

	/*
	 * C'tor
	 * Creates the InputFormat and the RecordReader object
	 */	
	public SequenceFileAccessor(InputData input) throws Exception
	{

		super(input,
		      new SequenceFileInputFormat<LongWritable, Writable>());

		Log = LogFactory.getLog(SequenceFileAccessor.class);		
	}

	/*
	 * Override virtual method to create specialized record reader
	 */
	protected Object getReader(JobConf jobConf, InputSplit split) throws IOException
	{
		return new SequenceFileRecordReader(jobConf, (FileSplit)split);
	}

}
