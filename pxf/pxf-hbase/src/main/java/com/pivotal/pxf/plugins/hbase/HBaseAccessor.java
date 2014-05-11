package com.pivotal.pxf.plugins.hbase;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import com.pivotal.pxf.plugins.hbase.utilities.HBaseColumnDescriptor;
import com.pivotal.pxf.plugins.hbase.utilities.HBaseTupleDescription;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Accessor for HBase.
 * This class is responsible for opening the HBase table requested and
 * for iterating over its relevant fragments (regions) to return the relevant table's rows.
 * <p>
 * The table is divided into several splits. Each accessor instance is assigned a single split.
 * For each region, a Scan object is used to describe the requested rows.
 * <p>
 * The class supports filters using the {@link HBaseFilterBuilder}.
 * Regions can be filtered out according to input from {@link HBaseFilterBuilder}.
 */
public class HBaseAccessor extends Plugin implements ReadAccessor {
    private HBaseTupleDescription tupleDescription;
    private HTable table;
    private SplitBoundary split;
    private Scan scanDetails;
    private ResultScanner currentScanner;
    private byte[] scanStartKey;
    private byte[] scanEndKey;

    /**
     * The class represents a single split of a table
     * i.e. a start key and an end key
     */
    private class SplitBoundary {
        protected byte[] startKey;
        protected byte[] endKey;

        SplitBoundary(byte[] first, byte[] second) {
            startKey = first;
            endKey = second;
        }

        byte[] startKey() {
            return startKey;
        }

        byte[] endKey() {
            return endKey;
        }
    }

    /**
     * Constructs {@link HBaseTupleDescription} based on HAWQ table description and 
     * initializes the scan start and end keys of the HBase table to default values.
     *  
     * @param input query information, contains HBase table name and filter
     */
    public HBaseAccessor(InputData input) {
        super(input);

        tupleDescription = new HBaseTupleDescription(input);
        split = null;
        scanStartKey = HConstants.EMPTY_START_ROW;
        scanEndKey = HConstants.EMPTY_END_ROW;
    }

    /**
     * Opens the HBase table.
     * 
     * @return true if the current fragment (split) is 
     * available for reading and includes in the filter
     */
    @Override
    public boolean openForRead() throws Exception {
        openTable();
        createScanner();
        addTableSplit();

        return openCurrentRegion();
    }

    /**
     * Closes the HBase table.
     */
    @Override
    public void closeForRead() throws Exception {
        table.close();
    }

    /**
     * Returns the next row in the HBase table, null if end of fragment.
     */
    @Override
    public OneRow readNextObject() throws IOException {
        Result result;

        // while currentScanner can't return a new result
        while ((result = currentScanner.next()) == null) {
            currentScanner.close(); // close it
            return null; // no more rows on the split
        }

        return new OneRow(null, result);
    }

    private void openTable() throws IOException {
        table = new HTable(HBaseConfiguration.create(), inputData.getDataSource().getBytes());
    }

    /**
     * Creates a {@link SplitBoundary} of the table split 
     * this accessor instance is assigned to scan. 
     * The table split is constructed from the fragment metadata 
     * passed in {@link InputData#getFragmentMetadata()}. 
     * <p>
     * The function verifies the split is within user supplied range.
     * <p>
     * It is assumed, |startKeys| == |endKeys|
     * This assumption is made through HBase's code as well.
     */
    private void addTableSplit() {

        byte[] serializedMetadata = inputData.getFragmentMetadata();
        if (serializedMetadata == null) {
            throw new IllegalArgumentException("Missing fragment metadata information");
        }
        try {
            ByteArrayInputStream bytesStream = new ByteArrayInputStream(serializedMetadata);
            ObjectInputStream objectStream = new ObjectInputStream(bytesStream);

            byte[] startKey = (byte[]) objectStream.readObject();
            byte[] endKey = (byte[]) objectStream.readObject();

            if (withinScanRange(startKey, endKey)) {
            	split = new SplitBoundary(startKey, endKey);
            }
        } catch (Exception e) {
            throw new RuntimeException("Exception while reading expected fragment metadata", e);
        }
    }

    /**
     * Returns true if given start/end key pair is within the scan range.
     */
    private boolean withinScanRange(byte[] startKey, byte[] endKey) {
    	
    	// startKey <= scanStartKey
        if (Bytes.compareTo(startKey, scanStartKey) <= 0) {
        	// endKey == table's end or endKey >= scanStartKey
            if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) || 
                    Bytes.compareTo(endKey, scanStartKey) >= 0) {
                return true;
            }
        } else { // startKey > scanStartKey
        	// scanEndKey == table's end or startKey <= scanEndKey
            if (Bytes.equals(scanEndKey, HConstants.EMPTY_END_ROW) || 
                    Bytes.compareTo(startKey, scanEndKey) <= 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Creates the Scan object used to describe the query
     * requested from HBase.
     * As the row key column always gets returned, no need to ask for it.
     */
    private void createScanner() throws Exception {
        scanDetails = new Scan();
        // Return only one version (latest)
        scanDetails.setMaxVersions(1);

        addColumns();
        addFilters();
    }

    /**
     * Opens the region of the fragment to be scanned.
     * Updates the Scan object to retrieve only rows from that region.
     */
    private boolean openCurrentRegion() throws IOException {
        if (split == null) {
            return false;
        }

        scanDetails.setStartRow(split.startKey());
        scanDetails.setStopRow(split.endKey());

        currentScanner = table.getScanner(scanDetails);
        return true;
    }

    /**
     * Adds the table tuple description to {@link #scanDetails},
     * so only these fields will be returned.
     */
    private void addColumns() {
        for (int i = 0; i < tupleDescription.columns(); ++i) {
            HBaseColumnDescriptor column = tupleDescription.getColumn(i);
            if (!column.isKeyColumn()) // Row keys return anyway
            {
                scanDetails.addColumn(column.columnFamilyBytes(), column.qualifierBytes());
            }
        }
    }

    /**
     * Uses {@link HBaseFilterBuilder} to translate a filter string into a
     * HBase {@link Filter} object. The result is added as a filter to the
     * Scan object.
     * <p>
     * Uses row key ranges to limit split count.
     */
    private void addFilters() throws Exception {
        if (!inputData.hasFilter()) {
            return;
        }

        HBaseFilterBuilder eval = new HBaseFilterBuilder(tupleDescription);
        Filter filter = eval.getFilterObject(inputData.getFilterString());
        scanDetails.setFilter(filter);

        scanStartKey = eval.startKey();
        scanEndKey = eval.endKey();
    }
}
