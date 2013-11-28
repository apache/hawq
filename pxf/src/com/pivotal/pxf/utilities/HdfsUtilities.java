package com.pivotal.pxf.utilities;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

/*
 * HdfsUtilities class exposes helper methods for PXF classes
 */
public class HdfsUtilities
{
	private static Log Log = LogFactory.getLog(HdfsUtilities.class);
	private static CompressionCodecFactory factory = 
			new CompressionCodecFactory(new Configuration());
	
	/*
	 * Helper routine to get a compression codec class
	 */
	private static Class<? extends CompressionCodec> getCodecClass(
			Configuration conf, String name) {
		
		Class<? extends CompressionCodec> codecClass;
		try {
			codecClass = conf.getClassByName(name).asSubclass(CompressionCodec.class);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Compression codec " + name + " was not found.", e);
		}
		return codecClass;
	}
	
	/**
	 * Helper routine to get compression codec through reflection
	 * 
	 * @param conf configuration used for reflection
	 * @param name codec name
	 * @returns generated CompressionCodec
	 */
	public static CompressionCodec getCodec(Configuration conf, String name) {
		
		return (CompressionCodec) ReflectionUtils.newInstance(
				getCodecClass(conf, name),
				conf);
	}
	
	/**
	 * Helper routine to get compression codec class by path (file suffix)
	 * 
	 * @param path path of file to get codec for
	 * @return matching codec class for the path. null if no codec is needed.
	 */
	private static Class<? extends CompressionCodec> getCodecClassByPath(String path) {
		
		Class<? extends CompressionCodec> codecClass = null;
		CompressionCodec codec = factory.getCodec(new Path(path));
    	if (codec != null)
    		codecClass = codec.getClass();
		Log.debug((codecClass == null ? "No codec" : "Codec " + codecClass)  
				+ " was found for file " + path);
		return codecClass;
	}
	
	/**
	 * Returns true if the needed codec is splittable. 
	 * If no codec is needed returns true as well.
	 * @param path path of the file to be read
	 * @return if the codec needed for reading the specified path is splittable.
	 */
	public static boolean isSplittableCodec(Path path) {
		
		final CompressionCodec codec = factory.getCodec(path);
		if (null == codec) 
		{
			return true;
		}
		
		return codec instanceof SplittableCompressionCodec;
	}
	
	/**
	 * Checks if requests should be handle in a single thread or not.
	 * 
	 * @param inputData container holding all parameters
	 * @param isReadRequest read/write request
	 * @return if the request can be run in multithreaded mode.
	 */
	public static boolean isThreadSafe(InputData inputData) {

		Configuration conf = new Configuration();
		String dataDir = inputData.path();
		Class<? extends CompressionCodec> codecClass = null;
		
		if (!inputData.threadSafe()) {
			return false;
		}
		
		String writeCodec = inputData.compressCodec();
		if (writeCodec != null) {
			codecClass = HdfsUtilities.getCodecClass(conf, writeCodec);
		}
		else {
			codecClass = HdfsUtilities.getCodecClassByPath(dataDir);
		}
		
		/* bzip2 codec is not thread safe */
		if ((codecClass != null) && 
			(codecClass == BZip2Codec.class)) {
			return false;
		}
		
		return true;
	}
	
}
