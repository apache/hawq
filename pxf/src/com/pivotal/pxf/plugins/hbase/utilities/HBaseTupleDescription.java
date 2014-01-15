package com.pivotal.pxf.plugins.hbase.utilities;

import com.pivotal.pxf.api.utilities.ColumnDescriptor;
import com.pivotal.pxf.api.utilities.InputData;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/*
 * The class extends the tuple description provided by InputData
 * to usage of HBaseColumnDescription.
 *
 * This class also loads lookup table sent (optionally) by the
 * fragmenter.
 */
public class HBaseTupleDescription
{
	private Map<String, byte[]> tableMapping;
	private List<HBaseColumnDescriptor> tupleDescription;
	private InputData conf;

	public HBaseTupleDescription(InputData conf)
	{
		this.conf = conf;
		parseHBaseTupleDescription();
	}

	public int columns()
	{
		return tupleDescription.size();
	}

	public HBaseColumnDescriptor getColumn(int index)
	{
		return tupleDescription.get(index);
	}

	private void parseHBaseTupleDescription()
	{
		tupleDescription = new ArrayList<HBaseColumnDescriptor>();
		loadUserData();
		createTupleDescription();
	}

	@SuppressWarnings("unchecked")
	private void loadUserData()
	{
		try
		{
			byte[] serializedTableMappings = conf.getFragmentUserData();

			// No userdata means no mappings for our table in lookup table
			if (serializedTableMappings == null)
				return;

			ByteArrayInputStream bytesStream = new ByteArrayInputStream(serializedTableMappings);
			ObjectInputStream objectStream = new ObjectInputStream(bytesStream);
			tableMapping = (Map<String, byte[]>)objectStream.readObject();
		}
		catch (Exception e)
		{
			throw new RuntimeException("Exception while reading expected user data from HBase's fragmenter", e);
		}
	}

	private void createTupleDescription()
	{
		for (int i = 0; i < conf.columns(); ++i)
		{
			ColumnDescriptor column = conf.getColumn(i);
			tupleDescription.add(getHBaseColumn(column));
		}
	}

	private HBaseColumnDescriptor getHBaseColumn(ColumnDescriptor column)
	{
		if (!column.isKeyColumn() && hasMapping(column))
			return new HBaseColumnDescriptor(column, getMapping(column));
		return new HBaseColumnDescriptor(column);
	}

	private boolean hasMapping(ColumnDescriptor column)
	{
		return tableMapping != null &&
			   tableMapping.containsKey(column.columnName().toLowerCase());
	}

	private byte[] getMapping(ColumnDescriptor column)
	{
		return tableMapping.get(column.columnName().toLowerCase());
	}
}
