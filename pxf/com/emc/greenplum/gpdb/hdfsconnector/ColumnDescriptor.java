package com.emc.greenplum.gpdb.hdfsconnector;

import org.apache.hadoop.hbase.util.Bytes;

/* 
 * ColumnDescriptor describes one column in greenplum database.
 * Currently it means a name and a type (GPDB OID)
 */
public class ColumnDescriptor
{
    int gpdbColumnType;
    String gpdbColumnName;
	int gpdbColumnIndex;
	public static final String recordkeyName = "recordkey";

    public ColumnDescriptor(String name, int type, int index)
    {
        gpdbColumnType = type;
        gpdbColumnName = name;
		gpdbColumnIndex = index;
    }

    public ColumnDescriptor(ColumnDescriptor copy)
    {
        this.gpdbColumnType = copy.gpdbColumnType;
        this.gpdbColumnName = copy.gpdbColumnName;
		this.gpdbColumnIndex = copy.gpdbColumnIndex;
    }

    public String columnName()
    {
        return gpdbColumnName;
    }

    public int columnType()
    {
        return gpdbColumnType;
    }
	
	public int columnIndex()
    {
        return gpdbColumnIndex;
    }	
}

/*
 * ColumnDescriptor for HBase columns. In the case of HBase columns,
 * the column name must be in either of the following forms:
 * 1) columnfamily:qualifier - standard HBase column
 * 2) hbaserowkey - Row key column (case insensitive)
 */
class HBaseColumnDescriptor extends ColumnDescriptor
{
    byte[] columnFamily;
    byte[] qualifier;
    boolean isRowColumn;

    public HBaseColumnDescriptor(ColumnDescriptor copy)
    {
        super(copy);

        if (gpdbColumnName.compareToIgnoreCase(recordkeyName) == 0)
        {
            isRowColumn = true;
            return;
        }

        isRowColumn = false;

        int seperatorIndex = gpdbColumnName.indexOf(':');
        if (seperatorIndex == -1)
            throw new IllegalArgumentException("Illegal HBase column name " + gpdbColumnName);

        columnFamily = Bytes.toBytes(gpdbColumnName.substring(0, seperatorIndex));
        qualifier = Bytes.toBytes(gpdbColumnName.substring(seperatorIndex + 1));
    }

    public byte[] columnFamilyBytes()
    {
        return columnFamily;
    }

    public byte[] qualifierBytes()
    {
        return qualifier;
    }

    public boolean isRowColumn()
    {
        return isRowColumn;
    }
};
