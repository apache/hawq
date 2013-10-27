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
 * Specialization of HdfsSplittableDataAccessor for sequence files, and sequence files writer
 */
public class SequenceFileAccessor extends HdfsSplittableDataAccessor implements IWriteAccessor
{

	private Configuration conf;
	private FileContext fc;
	private Path file;
	private CompressionCodec codec;
	private CompressionType compressionType;
	private SequenceFile.Writer writer;
	private LongWritable key;

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

	// opens file for write
	public boolean openForWrite() throws Exception
	{
		FileSystem fs;
		Path parent;
		
		// construct the output stream
		conf = new Configuration();   
		file = new Path(inputData.getProperty("X-GP-DATA-PATH"));
		fs = file.getFileSystem(conf);
		fc = FileContext.getFileContext();

		// TODO: add compression
		compressionType = SequenceFile.CompressionType.NONE;
		codec = null;

		key = new LongWritable(inputData.segmentId());

		if (fs.exists(file))
		{
			throw new IOException("file " + file.toString() + " already exists, can't write data");
		}
		parent = file.getParent();
		if (!fs.exists(parent))
		{
			fs.mkdirs(parent);
			Log.debug("Created new dir " + parent.toString());
		}

		writer = null;
		return true;
	}

	public boolean writeNextObject(OneRow onerow) throws IOException 
	{
		// init writer on first approach here, based on onerow.getData type
		// TODO: verify data is serializable.
		if (writer == null)
		{
			Class valueClass = onerow.getData().getClass();
			// create writer - do not allow overwriting existing file
			writer = SequenceFile.createWriter(fc, conf, file, LongWritable.class, valueClass, 
					compressionType, codec, new SequenceFile.Metadata(), EnumSet.of(CreateFlag.CREATE));
		}    

		Writable writable = (Writable) onerow.getData();		
		try 
		{
			writer.append(key, writable);
		} catch (IOException e) 
		{
			Log.error("Failed to write data to file: " + e.getMessage());
			return false;
		}

		return true;
	}

	public void closeForWrite() throws Exception
	{
		if (writer != null)
		{
			writer.sync();
			/*
			 * From release 0.21.0 sync() is deprecated in favor of hflush(), 
			 * which only guarantees that new readers will see all data written to that point, 
			 * and hsync(), which makes a stronger guarantee that the operating system has flushed 
			 * the data to disk (like POSIX fsync), although data may still be in the disk cache.
			 */
			writer.hsync(); 
			writer.close();
		}
	}
}
