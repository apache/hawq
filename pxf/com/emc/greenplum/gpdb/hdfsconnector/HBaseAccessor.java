package com.emc.greenplum.gpdb.hdfsconnector;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

/*
 * The Bridge API accessor for gphbase protocol.
 * This class is responsible for opening the HBase table requested and
 * for iterating over its relevant splits(regions) to return the relevant table's rows.
 *
 * The table is divided into several splits. Each GP segment is responsible for 
 * several splits according to the split function selectTableSplits().
 * For each region, a Scan object is used to describe the requested rows.
 *
 * The class now supports filters using the HBaseFilterEvaluator.
 * Regions can be filtered out according to input from HBaseFilterEvaluator.
 */
class HBaseAccessor implements IHdfsFileAccessor
{	
	private HBaseMetaData conf;
	private HTableInterface table;
	private List<SplitBoundary> splits;
	private Scan scanDetails;
	private ResultScanner currentScanner;
	private int currentRegionIndex;
	private byte[] scanStartKey;
	private byte[] scanEndKey;

	/* 
	 * The class represents a single split of a table
	 * i.e. a start key and an end key
	 */
	private class SplitBoundary
	{
		protected byte[] startKey;
		protected byte[] endKey;

		SplitBoundary(byte[] first, byte[] second)
		{
			startKey = first;
			endKey = second;
		}

		byte[] startKey() {return startKey;}

		byte[] endKey() {return endKey;}
	}

	public HBaseAccessor(HBaseMetaData configuration)
	{
		conf = configuration;

		splits = new ArrayList<SplitBoundary>();
		currentRegionIndex = 0;
		scanStartKey = HConstants.EMPTY_START_ROW;
		scanEndKey = HConstants.EMPTY_END_ROW;
	}

	public boolean Open() throws Exception
	{
		openTable();
		createScanner();
		selectTableSplits();

		return openCurrnetRegion();
	}

	/*
	 * Close the table
	 */
	public void Close() throws Exception
	{
		table.close();
	}

	public OneRow LoadNextObject() throws IOException
	{
		Result result;

		while ((result = currentScanner.next()) == null) // while currentScanner can't return a new result
		{
			currentScanner.close(); // close it
			++currentRegionIndex; // open next region

			if (!openCurrnetRegion())
				return null; // no more splits on the list
		}

		return new OneRow(null, result);
	}

	private void openTable() throws IOException
	{
		table = new HTable(HBaseConfiguration.create(), conf.tableName().getBytes());
	}

	/*
	 * The function creates an array of start,end keys pairs for each 
	 * table split this segment is going to scan.
	 *
	 * Split selection is done by GP master
	 *
	 * The function verifies splits are within user supplied range
	 *
	 * It is assumed, |startKeys| == |endKeys|
	 * This assumption is made through HBase's code as well
	 * We also assume, HTableInterface is implemented using HTable
	 */
	private void selectTableSplits() throws IOException
	{
		NavigableMap<HRegionInfo, ServerName> regions = ((HTable)table).getRegionLocations();
		int i = 0;
		
		ArrayList<Integer> fragments = conf.getDataFragments();
		
		for (HRegionInfo region : regions.keySet()) 
		{
			if (fragments.contains(new Integer(i))) 
			{
				byte[] startKey = region.getStartKey();
				byte[] endKey = region.getEndKey();

				if (withinScanRange(startKey, endKey))
					splits.add(new SplitBoundary(startKey, endKey));	
			}
			++i;
		}
	}

	/* 
	 * returns true if given start/end key pair is within the scan range
	 */
	private boolean withinScanRange(byte[] startKey, byte[] endKey)
	{
		if (Bytes.compareTo(startKey, scanStartKey) <= 0) // startKey <= scanStartKey
		{
			if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) || // endKey == table's end
				Bytes.compareTo(endKey, scanStartKey) >= 0) // endKey >= scanStartKey
				return true;
		} else // startKey > scanStartKey
			if (Bytes.equals(scanEndKey, HConstants.EMPTY_END_ROW) || //  scanEndKey == table's end
				   Bytes.compareTo(startKey, scanEndKey) <= 0) // startKey <= scanEndKey
			return true;
		return false;
	}

	/*
	 * The function creates the Scan object used to describe the query 
	 * requested from HBase.
	 * As the row key column always gets returned, no need to ask for it
	 */
	private void createScanner() throws Exception
	{
		scanDetails = new Scan();
		// Return only one version (latest)
		scanDetails.setMaxVersions(1);

		addColumns();
		addFilters();
	}

	/*
	 * Open the region of index currentRegionIndex from splits list.
	 * Update the Scan object to retrieve only rows from that region.
	 */
	private boolean openCurrnetRegion() throws IOException
	{
		if (currentRegionIndex == splits.size())
			return false;

		SplitBoundary region = splits.get(currentRegionIndex);
		scanDetails.setStartRow(region.startKey());
		scanDetails.setStopRow(region.endKey());

		currentScanner = table.getScanner(scanDetails);
		return true;
	}

	private void addColumns()
	{
		// Set the scan to return only the columns requested
		for (int i = 0; i < conf.columns(); ++i)
		{
			HBaseColumnDescriptor column = (HBaseColumnDescriptor)conf.getColumn(i);
			if (!column.isRowColumn()) // Row keys return anyway
				scanDetails.addColumn(column.columnFamilyBytes(), column.qualifierBytes());
		}
	}

	/*
	 * Uses HBaseFilterEvaluator to translate a filter string into a 
	 * HBase Filter object. The result is added as a filter to the 
	 * Scan object
	 *
	 * use row key ranges to limit split count
	 */
	private void addFilters() throws Exception
	{
		if (!conf.hasFilter())
			return;

		HBaseFilterEvaluator eval = new HBaseFilterEvaluator(conf);
		Filter filter = eval.getFilterObject(conf.filterString());
		scanDetails.setFilter(filter);

		scanStartKey = eval.startKey();
		scanEndKey = eval.endKey();
	}
}
