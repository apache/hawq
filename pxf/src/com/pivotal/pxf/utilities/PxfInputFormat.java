package com.pivotal.pxf.utilities;

import java.io.IOException;
import java.net.UnknownServiceException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/*
 * Class is not intended to read a specific format, hence it implements a dummy getRecordReader
 * Instead, its purpose is to apply FileInputFormat.getSplits from one point in GPFusion and get the splits
 * which are valid for the actual InputFormats, since all of them we use inherit FileInputFormat but do not 
 * override getSplits.
 */
public class PxfInputFormat extends FileInputFormat  
{ 

	private CompressionCodecFactory compressionCodecs = null;

	
	public PxfInputFormat(JobConf conf) 
	{
	    compressionCodecs = new CompressionCodecFactory(conf);
	}
	
	/*
	 * Dummy implementation - must override since FileInputFormat does not implement and interface InputFormat
	 * defines this method
	 */
	public RecordReader getRecordReader(InputSplit split,
									    JobConf conf,
									    Reporter reporter) throws IOException
	{
		throw new UnknownServiceException("GPFusionInputFormat should not be used for reading data, but only for obtaining the splist of a file");
	}

	/* 
	 *  Return if this file can be split (HD-6863)  
	 */
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) 
	{
		final CompressionCodec codec = compressionCodecs.getCodec(filename);
		if (null == codec) 
		{
			return true;
	    }
		
	    return codec instanceof SplittableCompressionCodec;
	}
	
}
