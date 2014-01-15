package com.pivotal.pxf.plugins.hbase.utilities;

import java.util.Arrays;

import com.pivotal.pxf.api.utilities.ColumnDescriptor;
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

	public HBaseColumnDescriptor(ColumnDescriptor copy)
	{
		this(copy, copy.columnName().getBytes());
	}

	public HBaseColumnDescriptor(ColumnDescriptor copy, byte[] newColumnName)
	{
		super(copy);

		if (isKeyColumn())
			return;

		int seperatorIndex = getSeparatorIndex(newColumnName);

		columnFamily = Arrays.copyOfRange(newColumnName, 0, seperatorIndex);
		qualifier = Arrays.copyOfRange(newColumnName, seperatorIndex + 1, newColumnName.length);
	}

	public byte[] columnFamilyBytes()
	{
		return columnFamily;
	}

	public byte[] qualifierBytes()
	{
		return qualifier;
	}

	private int getSeparatorIndex(byte[] columnName)
	{
		for (int i = 0; i < columnName.length; ++i)
		{
			if (columnName[i] == ':')
				return i;
		}

		throw new IllegalArgumentException("Illegal HBase column name " +
										   Bytes.toString(columnName) +
										   ", missing :");
	}
}
