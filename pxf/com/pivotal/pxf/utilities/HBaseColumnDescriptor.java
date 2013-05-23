package com.pivotal.pxf.utilities;

import org.apache.hadoop.hbase.util.Bytes;

/*
 * ColumnDescriptor for HBase columns. In the case of HBase columns,
 * the column name must be in either of the following forms:
 * 1) columnfamily:qualifier - standard HBase column
 * 2) hbaserowkey - Row key column (case insensitive)
 */
public class HBaseColumnDescriptor extends ColumnDescriptor
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

