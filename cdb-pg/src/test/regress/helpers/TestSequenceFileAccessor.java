import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

import com.pivotal.pxf.accessors.SplittableFileAccessor;
import com.pivotal.pxf.utilities.HDFSMetaData;


/*
 * Specialization of SplittableFileAccessor for sequence files
 */
public class TestSequenceFileAccessor extends SplittableFileAccessor
{
	/*
	 * C'tor
	 * Creates the InputFormat and the RecordReader object
	 */	
	public TestSequenceFileAccessor(HDFSMetaData meta) throws Exception
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
