package com.pivotal.pxf.utilities;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * HBaseLookupTable will load a table's lookup information
 * from lookup table if exists.
 *
 * Data is returned as a map of string and byte array from GetMappings
 *
 * Once created, close() MUST be called to cleanup resources.
 */
public class HBaseLookupTable implements Closeable
{
	private static final byte[] LOOKUPTABLENAME = Bytes.toBytes("pxflookup");
	private static final byte[] LOOKUPCOLUMNFAMILY = Bytes.toBytes("mapping");

	// Setting up HTable pool. Limit pool size to 10 instead of Integer.MAX_VALUE
	private static final int POOLSIZE = 10;
	private static final Configuration hbaseConfiguration = initConfiguration();
	// initPool must be called after hbaseConfiguration is initialized.
	private static final HTablePool pool = initPool();

	private HBaseAdmin admin;
	private Map<byte[], byte[]> rawTableMapping;
	private Log Log;
	private HTableInterface lookupTable;

	public HBaseLookupTable() throws IOException
	{
		admin = new HBaseAdmin(hbaseConfiguration);
		Log = LogFactory.getLog(HBaseLookupTable.class);
	}

	public Map<String, byte[]> getMappings(String tableName) throws IOException
	{
		if (!lookupTableValid())
			return null;

		loadTableMappings(tableName);

		if (tableHasNoMappings())
			return null;

		return lowerCaseMappings();
	}

	public void close() throws IOException
	{
		admin.close();
	}

	private boolean lookupTableValid() throws IOException
	{
		return (admin.isTableAvailable(LOOKUPTABLENAME) &&
				admin.isTableEnabled(LOOKUPTABLENAME) &&
				lookupHasCorrectStructure());
	}

	private boolean lookupHasCorrectStructure() throws IOException
	{
		HTableDescriptor htd = admin.getTableDescriptor(LOOKUPTABLENAME);
		return htd.hasFamily(LOOKUPCOLUMNFAMILY);
	}

	private void loadTableMappings(String tableName) throws IOException
	{
		openLookupTable();
		loadMappingMap(tableName);
		closeLookupTable();
	}

	private boolean tableHasNoMappings()
	{
		return rawTableMapping == null || rawTableMapping.size() == 0;
	}

	private Map<String, byte[]> lowerCaseMappings()
	{
		Map<String, byte[]> lowCaseKeys = new HashMap<String, byte[]>();
		for (Map.Entry<byte[],byte[]> entry : rawTableMapping.entrySet())
			lowCaseKeys.put(lowerCase(entry.getKey()),
							entry.getValue());

		return lowCaseKeys;
	}

	private void openLookupTable() throws IOException
	{
		lookupTable = pool.getTable(LOOKUPTABLENAME);
	}

	private void loadMappingMap(String tableName) throws IOException
	{
		Get lookupRow = new Get(Bytes.toBytes(tableName));
		lookupRow.setMaxVersions(1);
		lookupRow.addFamily(LOOKUPCOLUMNFAMILY);
		Result row;

		row = lookupTable.get(lookupRow);
		rawTableMapping = row.getFamilyMap(LOOKUPCOLUMNFAMILY);
		Log.debug("lookup table mapping for " + tableName +
				  " has " + (rawTableMapping == null ? 0 : rawTableMapping.size()) + " entries");
	}

	private void closeLookupTable() throws IOException
	{
		lookupTable.close();
	}

	private String lowerCase(byte[] key)
	{
		return Bytes.toString(key).toLowerCase();
	}

	private static Configuration initConfiguration()
	{
		return HBaseConfiguration.create();
	}

	private static HTablePool initPool()
	{
		return new HTablePool(hbaseConfiguration, POOLSIZE);
	}
}
