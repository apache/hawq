package com.emc.greenplum.gpdb.hdfsconnector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
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
	private List<FragmentInfo> fragmentInfos;
	private Log Log;
	
	/*
	 * C'tor
	 */
	public HdfsDataFragmenter(BaseMetaData md) throws IOException
	{
		super(md);
		Log = LogFactory.getLog(HdfsDataFragmenter.class);

		jobConf = new JobConf(new Configuration(), HdfsDataFragmenter.class);
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
		
		GPFusionInputFormat fformat = new GPFusionInputFormat();
		fformat.setInputPaths(jobConf, path);
		InputSplit[] splits = fformat.getSplits(jobConf, 1);
		
		for (InputSplit split : splits)
		{	
			FileSplit fsp = (FileSplit)split;
			String filepath = fsp.getPath().toUri().getPath();
			filepath = filepath.substring(1); // hack - remove the '/' from the beginning - we"ll deal with this next 
			FragmentInfo fi = new FragmentInfo(filepath,
											   fsp.getLocations());
			
			fragmentInfos.add(fi);
		}
		
		//print the fragment list to log when in debug level
		Log.debug(FragmentInfo.listToString(fragmentInfos, path.toString()));

		return FragmentInfo.listToJSON(fragmentInfos);
	}
	
}