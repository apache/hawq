package com.pivotal.pxf.plugins.hdfs.fragmenters;

import java.io.IOException;
import java.util.List;

import com.pivotal.pxf.api.fragmenters.Fragment;
import com.pivotal.pxf.api.fragmenters.Fragmenter;
import com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities;
import com.pivotal.pxf.plugins.hdfs.utilities.PxfInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.pivotal.pxf.api.utilities.InputData;

/*
 * Fragmenter class for HDFS data resources
 *
 * Given an HDFS data source (a file, directory, or wild card pattern)
 * divide the data into fragments and return a list of them along with
 * a list of host:port locations for each.
 */
public class HdfsDataFragmenter extends Fragmenter
{	
	private	JobConf jobConf;

	/*
	 * C'tor
	 */
	public HdfsDataFragmenter(InputData md) throws IOException
	{
		super(md);

		jobConf = new JobConf(new Configuration(), HdfsDataFragmenter.class);
	}
	
	/*
	 * path is a data source URI that can appear as a file 
	 * name, a directory name  or a wildcard returns the data 
	 * fragments in json format
	 */
    @Override
	public List<Fragment> getFragments() throws Exception
	{
		InputSplit[] splits = getSplits(new Path(inputData.path()));
		
		for (InputSplit split : splits)
		{	
			FileSplit fsp = (FileSplit)split;
			
			/*
			 * HD-2547: If the file is empty, an empty split is returned:
			 * no locations and no length.
			 */
			if (fsp.getLength() <= 0)
				continue;
					
			String filepath = fsp.getPath().toUri().getPath();
			String[] hosts = fsp.getLocations();
			
			/*
			 * metadata information includes: file split's
			 * start, length and hosts (locations).
			 */
			byte[] fragmentMetadata = HdfsUtilities.prepareFragmentMetadata(fsp);

			filepath = filepath.substring(1); // hack - remove the '/' from the beginning - we'll deal with this next 
			Fragment fragment = new Fragment(filepath, hosts, fragmentMetadata);
			fragments.add(fragment);
		}
		
		return fragments;
	}

	private InputSplit[] getSplits(Path path) throws IOException 
	{
		PxfInputFormat format = new PxfInputFormat();
		PxfInputFormat.setInputPaths(jobConf, path);
		return format.getSplits(jobConf, 1);
	}
	
}
