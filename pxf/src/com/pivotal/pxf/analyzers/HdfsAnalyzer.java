package com.pivotal.pxf.analyzers;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.pivotal.pxf.accessors.IReadAccessor;
import com.pivotal.pxf.bridge.ReadBridge;
import com.pivotal.pxf.utilities.HdfsUtilities;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.PxfInputFormat;



/*
 * Analyzer class for HDFS data resources
 *
 * Given an HDFS data source (a file, directory, or wild card pattern)
 * return statistics about it (number of blocks, number of tuples, etc.)
 */
public class HdfsAnalyzer extends Analyzer
{
	private	JobConf jobConf;
	private FileSystem fs;
	private DataSourceStatsInfo stats;
	private Log Log;

	/*
	 * C'tor
	 */
	public HdfsAnalyzer(InputData md) throws IOException
	{
		super(md);
		Log = LogFactory.getLog(HdfsAnalyzer.class);

		jobConf = new JobConf(new Configuration(), HdfsAnalyzer.class);
		fs = FileSystem.get(jobConf);
	}

	/*
	 * path is a data source URI that can appear as a file
	 * name, a directory name  or a wildcard returns the data
	 * fragments in json format
	 */
	public DataSourceStatsInfo GetEstimatedStats(String datapath) throws Exception
	{
		long blockSize = 0;
		long numberOfBlocks = 0;

		Path	path = new Path("/" + datapath); //yikes! any better way?
		
		InputSplit[] splits = getSplits(path);
		
		long numberOfTuplesInBlock = getNumberOfTuplesInBlock(splits);
			
		for (InputSplit split : splits)
		{
			FileSplit fsp = (FileSplit)split;

			if (blockSize == 0) // blockSize wasn't updated yet
			{
				Path filePath = fsp.getPath();
				FileStatus fileStatus = fs.getFileStatus(filePath);
				if (fileStatus.isFile()) {
					blockSize = fileStatus.getBlockSize();
					break;
				}
			}
		}

		// if no file is in path (only dirs), get default block size
		if (blockSize == 0)
			blockSize = fs.getDefaultBlockSize(path);
		numberOfBlocks = splits.length;

		stats = new DataSourceStatsInfo(blockSize, numberOfBlocks, numberOfTuplesInBlock*numberOfBlocks);

		//print files size to log when in debug level
		Log.debug(DataSourceStatsInfo.dataToString(stats, path.toString()));

		return stats;
	}

	/*
	 * Calculate the number of tuples in a split (block)
	 * Reads one block from HDFS. Exception during reading will
	 * filter upwards and handled in AnalyzerResource
	 */
	private long getNumberOfTuplesInBlock(InputSplit[] splits) throws Exception
	{
		long tuples = -1; /* default  - if we are not able to read data */
		
		if (splits.length == 0) {
			return tuples;
		}
		
		/*
		 * metadata information includes: file split's
		 * start, length and hosts (locations).
		 */
		byte[] fragmentMetadata = HdfsUtilities.prepareFragmentMetadata((FileSplit)splits[0]);
		inputData.setFragmentMetadata(fragmentMetadata);
		
		IReadAccessor accessor = ReadBridge.getFileAccessor(inputData);
		
		if (accessor.openForRead());
		{
			tuples = 0;
			while (accessor.readNextObject() != null)
				tuples++;

			accessor.closeForRead();
		}

		return tuples;
	}

	private InputSplit[] getSplits(Path path) throws IOException
	{
		PxfInputFormat fformat = new PxfInputFormat(jobConf);
		PxfInputFormat.setInputPaths(jobConf, path);
		return fformat.getSplits(jobConf, 1);
	}

}
