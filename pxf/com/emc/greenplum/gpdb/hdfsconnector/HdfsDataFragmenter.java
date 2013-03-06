package com.emc.greenplum.gpdb.hdfsconnector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;


/*
 * Fragmenter class for HDFS data resources
 *
 * Given an HDFS data source (a file, directory, or wild card pattern)
 * divide the data into fragments and return a list of them along with
 * a list of host:port locations for each.
 */
public class HdfsDataFragmenter extends BaseDataFragmenter
{	
	private	JobConf jobConf;
	private FileSystem fs;
	private List<FragmentInfo> fragmentInfos;
	private DataSourceStatsInfo filesSizeInfo;
	private Log Log;
	
	/*
	 * C'tor
	 */
	public HdfsDataFragmenter(BaseMetaData md) throws IOException
	{
		super(md);
		Log = LogFactory.getLog(HdfsDataFragmenter.class);

		jobConf = new JobConf(new Configuration(), HdfsDataFragmenter.class);
		fs = FileSystem.get(jobConf);
		fragmentInfos = new ArrayList<FragmentInfo>();
	}
	
	/*
	 * path is a data source URI that can appear as a file 
	 * name, a directory name  or a wildcard returns the data 
	 * fragments in json format
	 */	
	public String GetFragments(String datapath) throws Exception
	{
		Path	path = new Path("/" + datapath); //yikes! any better way?		

		InputSplit[] splits = getSplits(path);
		
		for (InputSplit split : splits)
		{	
			FileSplit fsp = (FileSplit)split;
			String filepath = fsp.getPath().toUri().getPath();
			filepath = filepath.substring(1); // hack - remove the '/' from the beginning - we'll deal with this next 
			FragmentInfo fi = new FragmentInfo(filepath,
											   fsp.getLocations());
			
			fragmentInfos.add(fi);
		}
		
		//print the fragment list to log when in debug level
		Log.debug(FragmentInfo.listToString(fragmentInfos, path.toString()));

		return FragmentInfo.listToJSON(fragmentInfos);
	}
	
	/*
	 * path is a data source URI that can appear as a file 
	 * name, a directory name  or a wildcard returns the data 
	 * fragments in json format
	 */	
	public String GetStats(String datapath) throws Exception
	{
		long blockSize = 0;
		long numberOfBlocks = 0;
		long numberOfTuples = -1L; // not implemented yet
		
		Path	path = new Path("/" + datapath); //yikes! any better way?
		
		InputSplit[] splits = getSplits(path);
		
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
			blockSize = fs.getDefaultBlockSize();
		numberOfBlocks = splits.length;
		
		filesSizeInfo = new DataSourceStatsInfo(blockSize, numberOfBlocks, numberOfTuples);
		
		//print files size to log when in debug level
		Log.debug(DataSourceStatsInfo.dataToString(filesSizeInfo, path.toString()));

		return DataSourceStatsInfo.dataToJSON(filesSizeInfo);
	}
	
	private InputSplit[] getSplits(Path path) throws IOException 
	{
		GPFusionInputFormat fformat = new GPFusionInputFormat();
		fformat.setInputPaths(jobConf, path);
		return fformat.getSplits(jobConf, 1);
	}
	
}