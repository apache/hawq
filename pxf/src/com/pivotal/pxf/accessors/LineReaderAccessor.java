package com.pivotal.pxf.accessors;

import java.io.IOException;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.TextInputFormat;

import com.pivotal.pxf.utilities.InputData;

/*
 * Specialization of HdfsSplittableDataAccessor for \n delimited files
 */
public class LineReaderAccessor extends HdfsSplittableDataAccessor
{
	/*
	 * C'tor
	 * Creates the LineReaderAccessor and the LineRecordReader object
	 */
	public LineReaderAccessor(InputData input) throws Exception
	{
		super(input,
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
