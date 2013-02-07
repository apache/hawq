package com.emc.greenplum.gpdb.hdfsconnector;

import java.util.ArrayList;

/*
 * Class for passing HBase parameters to the GPHBaseBridge
 */
public class HBaseMetaData extends HDMetaData
{
    protected String tableName;
	protected boolean filterStringValid;
	protected String filterString;

	/*
	 * C'tor for passing HDFS parameters to the GPHdfsBridge
	 */	
	public HBaseMetaData(HDMetaData copy)
	{
		super(copy);

        tableName = getProperty("X-GP-DATA-DIR");
		filterStringValid = getBoolProperty("X-GP-HAS-FILTER");

		if (filterStringValid)
			filterString = getProperty("X-GP-FILTER");
			
		recordkeyColumn = null;
	}

	/*
	 * Returns table name
	 */	
	public String tableName()
	{
		return tableName;
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

	/*
	 * Sets the columns description for HBase record
	 */	
	protected void parseHBaseColumns()
	{
		ArrayList<ColumnDescriptor> hbaseColumns = new ArrayList<ColumnDescriptor>();

        for (int i = 0; i < tupleDescription.size(); ++i)
        {
			hbaseColumns.add(new HBaseColumnDescriptor(tupleDescription.get(i)));
        }

		tupleDescription = hbaseColumns;
	}
}
