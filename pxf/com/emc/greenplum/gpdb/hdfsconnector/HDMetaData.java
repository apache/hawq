package com.emc.greenplum.gpdb.hdfsconnector;

import com.emc.greenplum.gpdb.hadoop.io.GPDBWritable;

import java.util.ArrayList;
import java.util.Map;
import java.io.FileOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

enum OutputFormat
{
    FORMAT_TEXT,
    FORMAT_GPDB_WRITABLE
}

/*
 * Common configuration for gphdfs/gphbase java code
 * Provides read-only access to common parameters supplied using system properties
 *
 * Upon construction, parses greenplum.* system properties
 */


public class HDMetaData extends BaseMetaData
{
    protected int segmentId;
    protected int totalSegments;
    protected OutputFormat outputFormat;
	protected String host;
    protected int port;
	protected ArrayList<ColumnDescriptor> tupleDescription;
	protected boolean filterStringValid;
	protected String filterString;
	protected BridgeProtocols protocol;
	protected ArrayList<Integer> dataFragments;
	private   Log Log;
		
	/* 
	 * The name of the recordkey column. It can appear in any location in the columns list.
	 * By specifying the recordkey column, the user declares that he is interested to receive for every record 
	 * retrieved also the the recordkey in the database. The recordkey is present in HBase table (it is called rowkey),
	 * and in sequence files. When the HDFS storage element queried will not have a recordkey and
	 * the user will still specify it in the "create external table" statement, then the values for this field will be null.
	 * This field will always be the first field in the tuple returned
	 */
	protected ColumnDescriptor recordkeyColumn;

    /* Constructor of HDMetaData
     * Parses greenplum.* configuration variables
     */
	public HDMetaData(Map<String, String> paramsMap)
	{
		super(paramsMap);
		Log = LogFactory.getLog(HDMetaData.class);
		
		// Store alignment for global use as a system property
		System.setProperty("greenplum.alignment", getProperty("X-GP-ALIGNMENT"));

        segmentId = getIntProperty("X-GP-SEGMENT-ID");
        totalSegments = getIntProperty("X-GP-SEGMENT-COUNT");
		
		filterStringValid = getBoolProperty("X-GP-HAS-FILTER");
		
		if (filterStringValid)
			filterString = getProperty("X-GP-FILTER");
		
		//TEMPORARY HACK! parseProtocol should be removed altogether.
		//here we use FRAGMENTER instead of HDTYPE for the time being
		//as an ugly workaround until the notions of 'base' and 'hdfs'
		//are removed.
		parseProtocol(getProperty(/*"X-GP-HDTYPE"*/"X-GP-FRAGMENTER"));
		
		parseFormat(getProperty("X-GP-FORMAT"));

		host = getProperty("X-GP-URL-HOST");
		port = getIntProperty("X-GP-URL-PORT");

		tupleDescription = new ArrayList<ColumnDescriptor>();
		recordkeyColumn = null;
        parseTupleDescription();

		dataFragments    = new ArrayList<Integer>();
		parseDataFragments(getProperty("X-GP-DATA-FRAGMENTS"));
	}

    /* Copy constructor of HDMetaData
     * Used to create from an extending class
     */
    public HDMetaData(HDMetaData copy)
    {
		super(copy);
		
        // TODO: need to switch to Configuration class, enables better property management.
		this.segmentId = copy.segmentId;
		this.totalSegments = copy.totalSegments;
		this.outputFormat = copy.outputFormat;
		this.host = copy.host;
		this.port = copy.port;
		this.tupleDescription = copy.tupleDescription;
		this.protocol = copy.protocol;
		this.dataFragments = copy.dataFragments;
		this.recordkeyColumn = copy.recordkeyColumn;
		this.filterStringValid = copy.filterStringValid;
		this.filterString = copy.filterString;
    }

    /* returns the number of segments in GP
     */
    public int totalSegments()
    {
        return totalSegments;
    }

    /* returns the current segment ID
     */
    public int segmentId()
    {
        return segmentId;
    }

    /* returns the current outputFormat
     * currently either text or gpdbwritable
     */
    public OutputFormat outputFormat()
    {
        return outputFormat;
    }

    /* returns the server name providing the service
     */
    public String serverName()
    {
        return host;
    }

    /* returns the server port providing the service
     */
    public int serverPort()
    {
        return port;
    }
	
	/*
	 * Returns true if there is a filter string to parse
	 */
	public boolean hasFilter()
	{
		return filterStringValid;
	}
	
	/*
	 * The filter string
	 */
	public String filterString()
	{
		return filterString;
	}	

	/* returns the number of columns in Tuple Description
	 */
	public int columns()
	{
		return tupleDescription.size();
	}

	/* returns column index from Tuple Description
	 */
	public ColumnDescriptor getColumn(int index)
	{
		return tupleDescription.get(index);
	}

	/* 
	 * returns the number of data fragments that were allocated to
	 * the GP segment attached to this GP Bridge
	 */
	public int dataFragmentsSize()
	{
		return dataFragments.size();
	}

	/* returns a data fragment
	 */
	public int getDataFragment(int index)
	{
		return dataFragments.get(index).intValue();
	}
	
	public ArrayList<Integer> getDataFragments()
	{
		return dataFragments;
	}
	
	/* returns the currently used protocol enum
	 */
	public BridgeProtocols protocol()
	{
		return protocol;
	}
	
	/*
	 * returns the column descriptor of the recordkey column.
	 * If the recordkey column was not specified by the user in the create table statement,
	 * then getRecordkeyColumn will return null.
	 */
	public ColumnDescriptor getRecordkeyColumn()
	{
		return recordkeyColumn;
	}

	/* 
     * Sets the protocol type based on input string
     */		
	protected void parseProtocol(String /*scheme*/ fragmenter)
	{
		if (fragmenter.compareToIgnoreCase("HBaseDataFragmenter") == 0)
			protocol = BridgeProtocols.GPHBASE;
		else
			protocol = BridgeProtocols.GPHDFS; /* default */
	}

	/* 
     * Sets the format type based on input string
     */			
    protected void parseFormat(String formatString)
    {
		if (formatString.equals("TEXT"))
			outputFormat = OutputFormat.FORMAT_TEXT;
		else if (formatString.equals("GPDBWritable"))
			outputFormat = OutputFormat.FORMAT_GPDB_WRITABLE;
		else
			throw new IllegalArgumentException("Wrong value for greenplum.format " + formatString);
    }

	/* 
     * Fills the list of the allocated data fragments
	 * The input string contains a list of indexes. Example: "3,8,11,12"
     */			
    protected void parseDataFragments(String fragments)
    {
		String [] toks = null;
		int toks_length = 0;

		/*
		 * 1. Test for empty string
		 */
		if (fragments.isEmpty())
			throw new IllegalArgumentException("Data fragments string received from the GP segment is empty");

		/*
		 * 2. Tokenize and insert into container as integers
		 */
		toks = fragments.split(",");
		toks_length = toks.length;
		for(int i = 0; i < toks_length; i++)
			dataFragments.add(new Integer(toks[i]));
    }

	/* 
     * Sets the tuple description for the record
     */		
    void parseTupleDescription()
    {		
        int columns = getIntProperty("X-GP-ATTRS");
        for (int i = 0; i < columns; ++i)
        {
            String columnName = getProperty("X-GP-ATTR-NAME" + i);
            int columnType = getIntProperty("X-GP-ATTR-TYPE" + i);
            ColumnDescriptor column = new ColumnDescriptor(columnName, columnType, i);
            tupleDescription.add(column);
			
			if (columnName.equalsIgnoreCase(ColumnDescriptor.recordkeyName))
				recordkeyColumn = column;						
        }		
    }
}
