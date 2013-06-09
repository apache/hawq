package com.pivotal.pxf.fragmenters;

import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import com.pivotal.pxf.utilities.BaseMetaData;

/*
 * Fragmenter class for HBase data resources.
 *
 * Extends the Fragmenter abstract class, with the purpose of transforming
 * an input data path (an HBase table name in this case) into a list of regions
 * that belong to this table. The result is a FragmentsOutput object, 
 * to be serialized in JSON.
 */
public class HBaseDataFragmenter extends Fragmenter
{		
	private Log Log;

	public HBaseDataFragmenter(BaseMetaData inConf)
	{
		super(inConf);
		Log = LogFactory.getLog(HBaseDataFragmenter.class);
	}

	public FragmentsOutput GetFragments(String datapath) throws Exception
	{

		//get a handle on the table by passing a table name
		HTable t = new HTable(HBaseConfiguration.create(), datapath);

		//get an info map of all regions of the table
		NavigableMap<HRegionInfo, ServerName> locations = t.getRegionLocations();

		//add each region (fragment) to the fragment list
		for (Entry<HRegionInfo, ServerName> entry: locations.entrySet()) 
		{
			ServerName svrname = entry.getValue();
		
			String sourceName = datapath;
			String[] hosts = new String[] {svrname.getHostname()};
			
			fragments.addFragment(sourceName, hosts);
		}

		//free table resources
		t.close();
		
        return fragments;


	}
}
