package com.pivotal.pxf.accessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.pivotal.pxf.filtering.HBaseFilterBuilder;
import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.resolvers.IReadResolver;
import com.pivotal.pxf.utilities.HBaseColumnDescriptor;
import com.pivotal.pxf.utilities.HBaseTupleDescription;
import com.pivotal.pxf.utilities.InputData;
import com.pivotal.pxf.utilities.Plugin;

/*
 * The Bridge API accessor for HBase.
 * This class is responsible for opening the HBase table requested and
 * for iterating over its relevant fragments(regions) to return the relevant table's rows.
 *
 * The table is divided into several splits. Each GP segment is responsible for
 * several splits according to the split function selectTableSplits().
 * For each region, a Scan object is used to describe the requested rows.
 *
 * The class now supports filters using the HBaseFilterBuilder.
 * Regions can be filtered out according to input from HBaseFilterBuilder.
 */
public class HBaseAccessor extends Plugin implements IReadAccessor 
{
	private HBaseTupleDescription tupleDescription;
	private HTable table;
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

	public HBaseAccessor(InputData input)
	{
		super(input);
		
		tupleDescription = new HBaseTupleDescription(input);
		splits = new ArrayList<SplitBoundary>();
		currentRegionIndex = 0;
		scanStartKey = HConstants.EMPTY_START_ROW;
		scanEndKey = HConstants.EMPTY_END_ROW;
	}

	public boolean openForRead() throws Exception
	{
		openTable();
		createScanner();
		selectTableSplits();

		return openCurrnetRegion();
	}

	/*
	 * Close the table
	 */
	public void closeForRead() throws Exception
	{
		table.close();
	}

	public OneRow readNextObject() throws IOException
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
		table = new HTable(HBaseConfiguration.create(), inputData.tableName().getBytes());
	}

	/*
	 * The function creates an array of start,end keys pairs for each
	 * table split this Accessor instance is assigned to scan.
	 *
	 * The function verifies splits are within user supplied range
	 *
	 * It is assumed, |startKeys| == |endKeys|
	 * This assumption is made through HBase's code as well
	 */
	private void selectTableSplits() throws IOException
	{
		NavigableMap<HRegionInfo, ServerName> regions = table.getRegionLocations();
		int i = 0;

		int fragment = inputData.getDataFragment();

		for (HRegionInfo region : regions.keySet())
		{
			if (fragment == i)
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
		} 
		else // startKey > scanStartKey
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
		for (int i = 0; i < tupleDescription.columns(); ++i)
		{
			HBaseColumnDescriptor column = tupleDescription.getColumn(i);
			if (!column.isKeyColumn()) // Row keys return anyway
				scanDetails.addColumn(column.columnFamilyBytes(), column.qualifierBytes());
		}
	}

	/*
	 * Uses HBaseFilterBuilder to translate a filter string into a
	 * HBase Filter object. The result is added as a filter to the
	 * Scan object
	 *
	 * use row key ranges to limit split count
	 */
	private void addFilters() throws Exception
	{
		if (!inputData.hasFilter())
			return;

		HBaseFilterBuilder eval = new HBaseFilterBuilder(tupleDescription);
		Filter filter = eval.getFilterObject(inputData.filterString());
		scanDetails.setFilter(filter);

		scanStartKey = eval.startKey();
		scanEndKey = eval.endKey();
	}
}
