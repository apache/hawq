package com.pivotal.pxf.fragmenters;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.pxf.utilities.BaseMetaData;


/*
 * Base class for the data fragmenters
 */
public abstract class BaseDataFragmenter extends Fragmenter
{
	protected BaseMetaData conf;
	protected List<FragmentInfo> fragmentInfos;
	private Log Log;
	
	/*
	 * C'tor
	 */
	public BaseDataFragmenter(BaseMetaData inConf)
	{
		super(inConf);
		/* 
		 * The conf variable will be discarded once we remove all specialized MetaData classes and remain 
		 * only with BaseMetaData which wholds the sequence of properties
		 */
		conf = this.getMetaData();
		
		fragmentInfos = new ArrayList<FragmentInfo>();
		Log = LogFactory.getLog(BaseDataFragmenter.class);
	}
	
	/*
	 * Base wrapper function to get fragments.
	 * The FragmentInfo data is returned by each specific fragmenter,
	 * by the function GetFragmentInfos().
	 * Conversion of the data and serialization is done here. 
	 */
	public String GetFragments(String data) throws Exception
	{
		/* populate fragmentInfos with fragments data */
		GetFragmentInfos(data);
		
		/* HD-2550: convert host names to IPs */
		FragmentInfo.convertHostsToIPs(fragmentInfos);
		
		//print the fragment list to log when in debug level
		Log.debug(FragmentInfo.listToString(fragmentInfos, data));

		return FragmentInfo.listToJSON(fragmentInfos);
	}
	
	/*
	 * Returns specific fragment info from a fragmenter.
	 */
	protected abstract void GetFragmentInfos(String data) throws Exception;
}