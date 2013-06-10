package com.pivotal.pxf.fragmenters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;

import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.HBaseLookupTable;

/*
 * Fragmenter class for HBase data resources.
 *
 * Extends the Fragmenter abstract class, with the purpose of transforming
 * an input data path (an HBase table name in this case) into a list of regions
 * that belong to this table.
 *
 * Fragmenter also puts HBase lookup table information for the given
 * table (if exists) in fragments' user data field.
 */
public class HBaseDataFragmenter extends Fragmenter
{
	public HBaseDataFragmenter(InputData inConf)
	{
		super(inConf);
	}

	public FragmentsOutput GetFragments() throws Exception
	{
		FragmentsOutput fragments = new FragmentsOutput();

		byte[] userData = prepareUserData();
		addTableFragments(fragments, userData);

		return fragments;
	}

	private byte[] prepareUserData() throws IOException
	{
		HBaseLookupTable lookupTable = new HBaseLookupTable();
		Map<String, byte[]> mappings = lookupTable.getMappings(inputData.tableName());
		lookupTable.close();

		if (mappings != null)
			return serializeMap(mappings);

		return null;
	}

	private void addTableFragments(FragmentsOutput fragments, byte[] userData) throws IOException
	{
		HTable table = new HTable(HBaseConfiguration.create(), inputData.tableName());
		NavigableMap<HRegionInfo, ServerName> locations = table.getRegionLocations();

		for (Map.Entry<HRegionInfo, ServerName> entry: locations.entrySet())
			addFragment(fragments, entry, userData);

		table.close();
	}

	private void addFragment(FragmentsOutput fragments, 
							 Map.Entry<HRegionInfo, ServerName> entry, 
							 byte[] userData)
	{
		ServerName serverInfo = entry.getValue();
		String[] hosts = new String[] {serverInfo.getHostname()};

		fragments.addFragment(inputData.tableName(), hosts, userData);
	}

	private byte[] serializeMap(Map<String, byte[]> tableMappings) throws IOException
	{
		ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
		ObjectOutputStream objectStream = new ObjectOutputStream(byteArrayStream);
		objectStream.writeObject(tableMappings);

		byte[] serializedColumnList = byteArrayStream.toByteArray();
		return serializedColumnList;
	}
}
