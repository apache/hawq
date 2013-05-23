package com.pivotal.pxf.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * HBaseLookupTable will check to see if there is a remapping of the columns
 * in a special GPDB lookup table which resides in HBase.
 * If there are any columns it will replace the column list in HBaseMetaData 
 * with the new list otherwise it will just parse (using HBaseColumnDescriptor)
 * the old one.
 *
 * The class has a single utility function translateColumns().
 */
public class HBaseLookupTable
{
	
	private Log Log;
	
	// TODO: put this someplace else
	private static final byte[] LOOKUPTABLENAME = Bytes.toBytes("gpxflookup");
	private static final byte[] LOOKUPCOLUMNFAMILY = Bytes.toBytes("mapping");
	/*
	 * The parameter sets the size of the lookup table pool size.
	 * This allows POOLSIZE concurrent requests without allocating new resources
	 * for the lookup table (assuming the pool is full).
	 */
	private static final int POOLSIZE = 10;

	private static final Configuration hbaseConfiguration = initConfiguration();
	// initPool must be called after hbaseConfiguration is initialized.
	private static final HTablePool pool = initPool(); 
	

	private HBaseMetaData conf;
	private HTableInterface table;
	private Map<byte[], byte[]> tableMapping;

	public static HBaseMetaData translateColumns(HBaseMetaData conf) throws IOException
	{
		HBaseLookupTable lookup = new HBaseLookupTable(conf);
		return lookup.getNewMeta();
	}

	/*
	 * Helper class which will allow replacing the tupleDescription list
	 * with a new one constructed from HBaseColumnDescriptor(s) either 
	 * from an external source (lookup table) or from the internal list
	 */
	private class HBaseMetaDataRemap extends HBaseMetaData
	{
		HBaseMetaDataRemap(HBaseMetaData copy)
		{
			super(copy);
		}

		public void parseExternalColumns(ArrayList<ColumnDescriptor> unparsed)
		{
			ArrayList<ColumnDescriptor> parsedColumns = new ArrayList<ColumnDescriptor>();

			for (int i = 0; i < unparsed.size(); ++i)
				parsedColumns.add(new HBaseColumnDescriptor(unparsed.get(i)));

			tupleDescription = parsedColumns;
		}

		public void parseInternalColumns()
		{
			parseExternalColumns(tupleDescription);
		}
	}

	private HBaseLookupTable(HBaseMetaData configuration) throws IOException
	{
		Log = LogFactory.getLog(HBaseLookupTable.class);
		conf = configuration;
	}

	private HBaseMetaData getNewMeta() throws IOException
	{
		loadTableMapping();
		replaceMapping();
		return conf;
	}

	private void loadTableMapping() throws IOException
	{
		HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);
		boolean tableExists = verifyLookupTable(admin);
		admin.close();
		
		if (!tableExists)
		{
			Log.debug("lookup table " + Bytes.toString(LOOKUPTABLENAME) +
					  " doesn't exist, no mapping to do");
			return;
		}

		openLookupTable();
		loadMappingMap();
		closeLookupTable();
		if (!noNewColumns())
			makeLowerCaseColumns();
			
	}

	private boolean verifyLookupTable(HBaseAdmin admin) throws IOException
	{
		
		if (!admin.isTableAvailable(LOOKUPTABLENAME)) 
			return false;
		
		if (!admin.isTableEnabled(LOOKUPTABLENAME))
			return false;
	
		HTableDescriptor htd = admin.getTableDescriptor(LOOKUPTABLENAME);
		if (!htd.hasFamily(LOOKUPCOLUMNFAMILY))
			return false;
		
		Log.debug("lookup table " + Bytes.toString(LOOKUPTABLENAME) +
				  " with column family " + Bytes.toString(LOOKUPCOLUMNFAMILY) + 
				  " is available.");
		return true;
	}
	
	private void openLookupTable() throws IOException
	{
		table = pool.getTable(LOOKUPTABLENAME);
	}

	private void loadMappingMap() throws IOException
	{
		// Get the row from the lookup table for the requested table
		Get lookupRow = new Get(Bytes.toBytes(conf.tableName()));
		lookupRow.setMaxVersions(1);
		lookupRow.addFamily(LOOKUPCOLUMNFAMILY);
		Result row;
		
		row = table.get(lookupRow);
		tableMapping = row.getFamilyMap(LOOKUPCOLUMNFAMILY);
		Log.debug("lookup table mapping for " + conf.tableName() + 
				  " has " + (noNewColumns() ? 0 : tableMapping.size()) + " entries");
		
	}

	/*
	 * Change all keys in lookup table to lower case.
	 * This is done to match postgres field names, that
	 * are saved as lower case.
	 */
	private void makeLowerCaseColumns()
	{
		Map<byte[], byte[]> lowerCaseTableMapping = new HashMap<byte[], byte[]>();
		
		for (Map.Entry<byte[],byte[]> entry : tableMapping.entrySet()) {
			byte[] key = entry.getKey();
			String low = Bytes.toString(key).toLowerCase();
			lowerCaseTableMapping.put(Bytes.toBytes(low), entry.getValue());
		}
		tableMapping.clear();
		tableMapping.putAll(lowerCaseTableMapping);
	}
	
	private void closeLookupTable() throws IOException
	{
		table.close();
	}

	private void replaceMapping() throws IOException
	{
		HBaseMetaDataRemap newConf = new HBaseMetaDataRemap(conf);

		if (noNewColumns()) // if no columns found in the lookup table
			newConf.parseInternalColumns(); // just parse the current list
		else // otherwise
			newConf.parseExternalColumns(getNewColumns()); // parse a new list

		conf = newConf;
	}

	private boolean noNewColumns()
	{
		return tableMapping == null;
	}

	/*
	 * The function replaces columns that appear in the lookup table
	 */
	private ArrayList<ColumnDescriptor> getNewColumns()
	{
		ArrayList<ColumnDescriptor> newColumns = new ArrayList<ColumnDescriptor>();
		for (int i = 0; i < conf.columns(); ++i)
		{
			ColumnDescriptor column = conf.getColumn(i);
			byte[] columnNameBytes = Bytes.toBytes(column.columnName().toLowerCase());
			
			if (tableMapping.containsKey(columnNameBytes))
				newColumns.add(new ColumnDescriptor(Bytes.toString(tableMapping.get(columnNameBytes)),
													column.columnType(), i));
			else
				newColumns.add(column);
		}

		return newColumns;
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
