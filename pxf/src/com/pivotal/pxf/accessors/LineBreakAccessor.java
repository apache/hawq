package com.pivotal.pxf.accessors;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.format.SimpleText;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Utilities;

/*
 * Specialization of HdfsSplittableDataAccessor for \n delimited files
 */
public class LineBreakAccessor extends HdfsSplittableDataAccessor implements IWriteAccessor
{
	private DataOutputStream dos;
	private Configuration conf;
	private FileSystem fs;
	private Path file;

	private Log Log;
	/*
	 * C'tor
	 * Creates the LineReaderAccessor and the LineRecordReader object
	 */
	public LineBreakAccessor(InputData input) throws Exception
	{
		super(input,
			  new TextInputFormat());
		((TextInputFormat)fformat).configure(jobConf);
		
		Log = LogFactory.getLog(LineBreakAccessor.class);

 	}
	
	/*
	 * Override virtual method to create specialized record reader
	 */	
	protected Object getReader(JobConf jobConf, InputSplit split) throws IOException
	{
		return new LineRecordReader(jobConf, (FileSplit)split);
	}

	// opens file for write
	public boolean openForWrite() throws Exception
	{
		
		String fileName = inputData.getProperty("X-GP-DATA-PATH");
		String compressCodec = inputData.compressCodec();
		CompressionCodec codec = null;
		
		conf = new Configuration();
		fs = FileSystem.get(conf);		

		// get compression codec
		if (compressCodec != null)
		{
			// default codec is Gzip
			codec = (CompressionCodec) ReflectionUtils.newInstance(
					Utilities.getCodecClass(conf, compressCodec),
					conf);
			String extension = codec.getDefaultExtension();
			fileName += extension;
		}

		file = new Path(fileName);
		
		if (fs.exists(file))
		{
			throw new IOException("file " + file.toString() + " already exists, can't write data");
		}
		org.apache.hadoop.fs.Path parent = file.getParent();
		if (!fs.exists(parent))
		{
			fs.mkdirs(parent);
			Log.debug("Created new dir " + parent.toString());
		}

		// create output stream - do not allow overwriting existing file
		createOutputStream(file, codec);
	
		return true;
	}
	
	/*
	 * Create output stream from given file.
	 * If compression codec is provided, wrap it around stream. 
	 */
	private void createOutputStream(Path file, CompressionCodec codec) throws IOException
	{
		if (codec != null)
		{
			FSDataOutputStream fileOut = fs.create(file, false);
			dos = new DataOutputStream(codec.createOutputStream(fileOut));
		}
		else 
			dos = fs.create(file, false);
	}
	
	// write row into stream
	public boolean writeNextObject(OneRow onerow) throws Exception
	{
		
		SimpleText text = (SimpleText)onerow.getData();
		text.write(dos);
		
		return true;
	}

	public void closeForWrite() throws Exception
	{
		if (dos != null)
		{
			dos.flush();
			dos.close();
		}
	}
}
