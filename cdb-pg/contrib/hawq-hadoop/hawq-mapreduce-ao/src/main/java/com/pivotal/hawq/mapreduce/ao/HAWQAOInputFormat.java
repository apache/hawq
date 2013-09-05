package com.pivotal.hawq.mapreduce.ao;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.ao.db.Metadata;
import com.pivotal.hawq.mapreduce.ao.file.HAWQAOFileStatus;
import com.pivotal.hawq.mapreduce.ao.file.HAWQAOSplit;
import com.pivotal.hawq.mapreduce.conf.HAWQConfiguration;
import com.pivotal.hawq.mapreduce.file.HAWQFileStatus;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An InputFormat that reads input data from HAWQ table.
 * <p/>
 * HAWQInputFormat emits LongWritables containing the record number as key and
 * DBWritables as value.
 * 
 */
public final class HAWQAOInputFormat extends FileInputFormat<Void, HAWQRecord>
{
	private static final Log LOG = LogFactory.getLog(HAWQAOInputFormat.class);

	private static HAWQFileStatus[] filestatus = null;

	public static void setInput(Configuration conf, Metadata metadata)
	{
		HAWQConfiguration.setInputTableEncoding(conf,
				metadata.getTableEncoding());
		HAWQConfiguration.setInputTableSchema(conf, metadata.getSchema());
		filestatus = metadata.getFileStatus();
	}

	@Override
	public RecordReader<Void, HAWQRecord> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException
	{
		// For AO table, we return HAWQAORecordReader
		RecordReader<Void, HAWQRecord> recordReader = new HAWQAORecordReader();
		return recordReader;
	}

	/**
	 * Generate the list of files and make them into FileSplits.
	 * 
	 * @param job
	 *            the job context
	 * @throws IOException
	 */
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException
	{
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (int i = 0; i < filestatus.length; ++i)
		{
			HAWQAOFileStatus aofilestatus = null;
			try
			{
				aofilestatus = (HAWQAOFileStatus) filestatus[i];
			}
			catch (ClassCastException e)
			{
				throw new IOException("Failed to get file attribute from "
						+ filestatus[i].getClass().getName());
			}
			String pathStr = aofilestatus.getPathStr();
			long fileLength = aofilestatus.getFileLength();
			if (fileLength == 0)
				continue;

			boolean checksum = aofilestatus.getChecksum();
			String compressType = aofilestatus.getCompressType();
			int blocksize = aofilestatus.getBlockSize();
			Path path = new Path(pathStr);
			if (fileLength != 0)
			{
				FileSystem fs = path.getFileSystem(job.getConfiguration());
				BlockLocation[] blkLocations = fs.getFileBlockLocations(
						fs.getFileStatus(path), 0, fileLength);
				// not splitable
				splits.add(new HAWQAOSplit(path, 0, fileLength, blkLocations[0]
						.getHosts(), checksum, compressType, blocksize));
			}
			else
			{
				// Create empty hosts array for zero length files
				splits.add(new HAWQAOSplit(path, 0, fileLength, new String[0],
						checksum, compressType, blocksize));
			}
		}
		job.getConfiguration().setLong(NUM_INPUT_FILES, splits.size());
		LOG.debug("Total # of splits: " + splits.size());
		return splits;
	}

}
