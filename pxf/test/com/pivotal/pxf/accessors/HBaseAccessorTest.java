package com.pivotal.pxf.accessors;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertFalse;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pivotal.pxf.utilities.HBaseTupleDescription;
import com.pivotal.pxf.utilities.InputData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.util.NavigableMap;
import java.util.Set;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.TreeMap;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HBaseAccessor.class, HBaseConfiguration.class})
public class HBaseAccessorTest
{
	static final String tableName = "fishy HBase table";

	InputData inputData;
	HBaseTupleDescription tupleDescription;
	HTable table;
	Scan scanDetails;
	Configuration hbaseConfiguration;
	HBaseAccessor accessor;

	@After
	/*
	 * After each test is done, close the accessor 
	 * if it was created
	 */
	public void tearDown() throws Exception
	{
		if (accessor == null)
			return;

		closeAccessor();
		accessor = null;
	}

	@Test
	/*
	 * Test construction of HBaseAccessor.
	 * Actually no need for this as it is tested in all other tests
	 * constructing HBaseAccessor but it serves as a simple example 
	 * of mocking
	 *
	 * HBaseAccessor is created and then HBaseTupleDescriptioncreation 
	 * is verified
	 */
	public void construction() throws Exception
	{
		prepareConstruction();
		HBaseAccessor accessor = new HBaseAccessor(inputData);
		PowerMockito.verifyNew(HBaseTupleDescription.class).withArguments(inputData);
	}

	@Test
	@SuppressWarnings("unchecked")
	/*
	 * Test Open returns false when table has no regions
	 * 
	 * Done by returning an empty Map from getRegionLocations
	 * Verify Scan object doesn't contain any columns / filters
	 * Verify scan did not start
	 */
    public void tableHasNoRegions() throws Exception
	{
		prepareConstruction();
		prepareTableOpen();
		prepareEmptyScanner();

		NavigableMap<HRegionInfo, ServerName> emptyRegionsMap = new TreeMap<HRegionInfo, ServerName>();
		when(table.getRegionLocations()).thenReturn(emptyRegionsMap);

		accessor = new HBaseAccessor(inputData);

		assertFalse(accessor.openForRead());

		verifyScannerDidNothing();
	}

    @Test
	@SuppressWarnings("unchecked")
	/*
	 * Test Open returns false when fragment list doesn't contain
	 * any of the regions in the table
	 *
	 * Done by faking 3 items on the list (iterator.hasNext()
	 * returns true, true, true, false)
	 * To avoid creation of real HRegionInfo and ServerName, 
	 * Map, Set and Iterator are faked
	 *
	 * Verify mockFragments.contains was called 3 times
	 */
	public void noRegionsInFragmentsList() throws Exception
	{
		prepareConstruction();
		prepareTableOpen();
		prepareEmptyScanner();

		NavigableMap fakeRegionsMap = mock(NavigableMap.class);
		Set fakeRegionsSet = mock(Set.class);
		Iterator fakeIterator = mock(Iterator.class);
		// fake 3 items
		when(fakeIterator.hasNext()).thenReturn(true, true, true, false);

		when(table.getRegionLocations()).thenReturn(fakeRegionsMap);
		when(fakeRegionsMap.keySet()).thenReturn(fakeRegionsSet);
		when(fakeRegionsSet.iterator()).thenReturn(fakeIterator);

		// fragments list doesn't contain any region
		when(inputData.getDataFragment()).thenReturn(-1);

		accessor = new HBaseAccessor(inputData);
		assertFalse(accessor.openForRead());

		verifyScannerDidNothing();

    }

	/*
	 * Helper for test setup. 
	 * Creates a mock for HBaseTupleDescription and InputData
	 */
	private void prepareConstruction() throws Exception
	{
		inputData = mock(InputData.class);
		tupleDescription = mock(HBaseTupleDescription.class);
		PowerMockito.whenNew(HBaseTupleDescription.class).withArguments(inputData).thenReturn(tupleDescription);
	}

	/*
	 * Helper for test setup.
	 * Adds a table name and prepares for table creation
	 */
	private void prepareTableOpen() throws Exception
	{
		// Set table name
		when(inputData.tableName()).thenReturn(tableName);

		// Make sure we mock static functions in HBaseConfiguration
		PowerMockito.mockStatic(HBaseConfiguration.class);

		hbaseConfiguration = mock(Configuration.class);
		when(HBaseConfiguration.create()).thenReturn(hbaseConfiguration);
		table = mock(HTable.class);
		PowerMockito.whenNew(HTable.class).withArguments(hbaseConfiguration, tableName.getBytes()).thenReturn(table);
	}

	/*
	 * Helper for test setup.
	 * Sets zero columns (not realistic) and no filter
	 */
	private void prepareEmptyScanner() throws Exception
	{
		scanDetails = mock(Scan.class);
		PowerMockito.whenNew(Scan.class).withNoArguments().thenReturn(scanDetails);

		when(tupleDescription.columns()).thenReturn(0);
		when(inputData.hasFilter()).thenReturn(false);
	}

	/*
	 * Verify Scan object was used but didn't do much
	 */
	private void verifyScannerDidNothing() throws Exception
	{
		// setMaxVersions was called with 1
		verify(scanDetails).setMaxVersions(1);
		// addColumn was not called
		verify(scanDetails, never()).addColumn(any(byte[].class), any(byte[].class));
		// addFilter was not called
		verify(scanDetails, never()).setFilter(any(org.apache.hadoop.hbase.filter.Filter.class));
		// Nothing else was missed
		verifyNoMoreInteractions(scanDetails);
		// Scanner was not used
		verify(table, never()).getScanner(scanDetails);
	}

	/*
	 * Close the accessor and make sure table was closed
	 */
	private void closeAccessor() throws Exception
	{
		accessor.closeForRead();
		verify(table).close();
	}
}
