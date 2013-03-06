package com.emc.greenplum.gpdb.hdfsconnector;

import java.util.ArrayList;

/*
 * Class for passing HBase parameters to the GPHBaseBridge
 */
public class HBaseMetaData extends HDMetaData
{
    protected String tableName;

	/*
	 * C'tor for passing HDFS parameters to the GPHdfsBridge
	 */	
	public HBaseMetaData(HDMetaData copy)
	{
		super(copy);

        tableName = getProperty("X-GP-DATA-DIR");
			
		recordkeyColumn = null;
	}

	/*
	 * Returns table name
	 */	
	public String tableName()
	{
		return tableName;
	}
}
