package com.emc.greenplum.gpdb.hdfsconnector;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;

/*
 * Implementation of IHdfsFileAccessor for accessing a sequence file. A sequence file is splittable
 * - it means that HDFS will divide the file into splits based on an internal algorithm (by default, the 
 * the block size 64 MB is also the split size). This code runs in the context of the java process 
 * spawned by a GPDB segment and so it will only process the splits that correspond to its segment id - 
 * exact algorithm in the Open() function. While fetching each record, this class needs to go over the records
 * located in all its splits.
 */
public abstract class SplittableFileAccessor implements IHdfsFileAccessor
{
	protected Configuration conf = null;
	protected HDFSMetaData metaData = null;
	private LinkedList<InputSplit> segSplits = null;
	protected RecordReader<Object, Object> reader = null;
	protected FileInputFormat<?, ?> fformat = null;
	ListIterator<InputSplit> iter = null;
	private InputSplit currSplit = null;
	protected JobConf jobConf = null;
	Object key, data;

	/*
	 * C'tor
	 */ 
	public SplittableFileAccessor(HDFSMetaData meta, 
						          FileInputFormat<?, ?> inFormat) throws Exception
	{
        metaData = meta;
		fformat = inFormat;

		// 1. Load Hadoop configuration defined in $HADOOP_HOME/conf/*.xml files
		conf = new Configuration();

		// 2. variable required for the splits iteration logic 
		jobConf = new JobConf(conf, SplittableFileAccessor.class);
	}

	/*
	 * Open
	 * Fetches the first split (relevant to this segment) in the file, using
	 * the splittable API - InputFormat
	 */	
	public boolean Open() throws Exception
	{
		// 1. get the list of all splits the input file has
		int segs = metaData.totalSegments();
        int segId = metaData.segmentId();
		fformat.setInputPaths(jobConf, new Path(metaData.path()));
		/*
		 * We ask for 1 split, but this doesn't mean that we are always going to get 1 split
		 * The number of splits depends on the data size. What we assured here is that always,
		 * the split size will be equal to the block size. To understand this, one should look
		 * at the implementation of FileInputFormat.getSplits()
		 */
		InputSplit[] splits = fformat.getSplits(jobConf, 1);
		int actual_splits_size = splits.length;
		int allocated_splits_size = metaData.dataFragmentsSize(); // it's called dataFragments because it represents both hdfs and hbase

		// 2. from all the splits choose only those that correspond to this segment id
		segSplits = new LinkedList<InputSplit>();
		for (int i = 0; i < allocated_splits_size; i++)
		{
			int alloc_split_idx = metaData.getDataFragment(i);
			
			/*
			 * Testing for the case where between the time of the GP Master meta
			 * data retrieval and now, the file was deleted or replaced by a smaller file.
			 * This is an extreme case which shouldn't happen - but we want to make sure
			 */
			if (alloc_split_idx < actual_splits_size)
				segSplits.add(splits[alloc_split_idx]);			
		}
		// 3. Initialize record reader based on current split
		iter = segSplits.listIterator(0);
		return getNextSplit();	
	}
	
	/*
	 * Specialized accessors will override this method and implement their own recordReader
	 */
	abstract protected Object getReader(JobConf jobConf, InputSplit split) throws IOException;

	/*
	 * getNextSplit
	 * Sets the current split and initializes a RecordReader who feeds from the split
	 */
	@SuppressWarnings(value = "unchecked")
	protected boolean getNextSplit() throws IOException
	{
		if (!iter.hasNext())
			return false;
		
		currSplit = iter.next();
		reader = (RecordReader<Object, Object>)getReader(jobConf, currSplit);	 // approach the specialized class override function	
		key = reader.createKey(); 
		data = reader.createValue();
		return true;
	}

	/*
	 * LoadNextObject
	 * Fetches one record from the  file. The record is returned as a Java object.
	 */		
	public OneRow LoadNextObject() throws IOException
	{
		if (!reader.next(key, data)) // if there is one more record in the current split
		{
			if (getNextSplit()) // the current split is exhausted. try to move to the next split.
			{
				if (!reader.next(key, data)) // read the first record of the new split
					return null; // make sure we return nulls 
			}
			else
				return null; // make sure we return nulls 
		}
		
		/*
		 * if neither condition was met, it means we already read all the records in all the splits, and
		 * in this call record variable was not set, so we return null and thus we are signaling end of 
		 * records sequence
		*/
		return new OneRow(key, data);
	}

	/*
	 * Close
	 * When user finished reading the file, it closes the RecordReader
	 */		
	public void Close() throws Exception
	{
		if (reader != null)
			reader.close();
	}	
}
