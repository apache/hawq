package com.pivotal.pxf.utilities;


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
