import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.TimeZone;

/*
 * HBaseCreateTable creates a table named dataTableName with numberOfSplits splits in HBase.
 * Meaning it will have numberOfSplits + 1 regions.
 * Then, each split is loaded with rowsPerSplit rows using the prefix rowKeyPrefix for row key
 * and valuePrefix for value.
 */
class HBaseCreateTable
{
	Configuration config;
	HBaseAdmin admin;

	String dataTableName;
	final int numberOfSplits = 2; // use 3 regions.
	final String columnFamilyName = "cf1";
	final String qualifierPrefix = "ql";
	int rowsPerSplit;
	boolean useNull;
	boolean upperCaseLookup;
	String rowKeyPrefix = "row";
	final String valuePrefix = "value";
	final String lookupTableName = "pxflookup";
	final String lookupTableMappingColumnFamilyName = "mapping";

	void go() throws Exception
	{
		if (tableExists(dataTableName))
			throw new Exception("table " + dataTableName + " already exists");

		printStep("create table");
		createTable();

		printStep("populate table");
		ArrayList<Put> rows = generateRows();
		populateTable(rows);

		printStep("update lookup table");
		updateLookup();

		printStep("leave");
	}

	HBaseCreateTable(String tableName, int rowsPerSplit, boolean useNull, boolean upperCaseLookup) throws Exception
	{
		dataTableName = tableName;
		this.rowsPerSplit = rowsPerSplit;
		this.useNull = useNull;
		this.upperCaseLookup = upperCaseLookup;
		
		config = HBaseConfiguration.create();
		admin = new HBaseAdmin(config);
	}

	void setRowKeyPrefix(String rowKeyPrefix)
	{
		this.rowKeyPrefix = rowKeyPrefix;
	}
	
	boolean tableExists(String tableName) throws IOException
	{
		return admin.isTableAvailable(tableName);
	}

	void createTable() throws IOException
	{
		String[] splits = generateSplits();

		HTableDescriptor tableDescription = new HTableDescriptor(dataTableName);
		tableDescription.addFamily(new HColumnDescriptor(columnFamilyName));

		admin.createTable(tableDescription,
						  Bytes.toByteArrays(splits));
	}

	String[] generateSplits()
	{
		String[] splits = new String[numberOfSplits];

		for (int i = 0; i < numberOfSplits; ++i)
			splits[i] = String.format("%s%08d", rowKeyPrefix, (i + 1) * rowsPerSplit);

		return splits;
	}

	ArrayList<Put> generateRows() throws java.io.UnsupportedEncodingException
	{
		byte[] columnFamily = Bytes.toBytes(columnFamilyName);
		ArrayList<Put> rows = new ArrayList<Put>();

		for (int splitIndex = 0; splitIndex < (numberOfSplits + 1); ++splitIndex)
		{
			for (int i = 0; i < rowsPerSplit; ++i)
			{
				//Row Key
				String rowKey = String.format("%s%08d", rowKeyPrefix, i + splitIndex * rowsPerSplit);
				Put newRow = new Put(Bytes.toBytes(rowKey));

				//Qualifier 1. regular ascii string
				if ((!useNull) || (i%2==0))
					addValue(newRow, columnFamily, "q1", String.format("ASCII%08d", i));

				//Qualifier 2. multibyte utf8 string.
                addValue(newRow, columnFamily, "q2", String.format("UTF8_計算機用語_%08d", i).getBytes());

				//Qualifier 3. integer value.
                if ((!useNull) || (i%3==0))
                	addValue(newRow, columnFamily, "q3", String.format("%08d", 1 + i + splitIndex * rowsPerSplit));

				//Qualifier 4. regular ascii (for a lookup table redirection)
                addValue(newRow, columnFamily, "q4", String.format("lookup%08d", i * 2));

                //Qualifier 5. real (float)
                addValue(newRow, columnFamily, "q5", String.format("%d.%d", i, i));

                //Qualifier 6. float (double)
                addValue(newRow, columnFamily, "q6", String.format("%d%d%d%d.%d", i, i, i, i, i));

                //Qualifier 7. bpchar (char)
                addValue(newRow, columnFamily, "q7", String.format("%c", (i + 32) % Character.MAX_VALUE));

                //Qualifier 8. smallint (short)
                addValue(newRow, columnFamily, "q8", String.format("%d", i));

                //Qualifier 9. bigint (long)
                Long value9 = ((i * i * i * 10000000000L + i) % Long.MAX_VALUE) * (long)Math.pow(-1, i % 2);
                addValue(newRow, columnFamily, "q9", value9.toString());

				//Qualifier 10. boolean
				addValue(newRow, columnFamily, "q10", Boolean.toString((i % 2) == 0));

				//Qualifier 11. numeric (string)
				addValue(newRow, columnFamily, "q11", (new Double(Math.pow(10, i))).toString());

				//Qualifier 12. Timestamp
				//Removing system timezone so tests will pass anywhere in the world :)
				int timeZoneOffset = TimeZone.getDefault().getRawOffset();
				addValue(newRow, columnFamily, "q12", (new Timestamp(6000 * i - timeZoneOffset)).toString());

				rows.add(newRow);
			}
		}

		return rows;
	}

    void addValue(Put row, byte[] cf, String ql, byte[] value)
    {
        row.add(cf, ql.getBytes(), value);
    }

    void addValue(Put row, byte[] cf, String ql, String value) throws java.io.UnsupportedEncodingException
    {
        addValue(row, cf, ql, value.getBytes("UTF-8"));
    }

	void populateTable(ArrayList<Put> rows) throws IOException
	{
		HTable table = new HTable(config, dataTableName);
		table.put(rows);
		table.close();
	}

	void printStep(String desc)
	{
		System.out.println(desc);
	}

	void updateLookup() throws Exception
	{
		if (!tableExists(lookupTableName))
			createLookupTable();

		HTable lookup = new HTable(config, lookupTableName);
		lookup.put(newMapping());
		lookup.close();
	}

	void createLookupTable() throws IOException
	{
		HTableDescriptor tableDescription = new HTableDescriptor(lookupTableName);
		tableDescription.addFamily(new HColumnDescriptor(lookupTableMappingColumnFamilyName));

		admin.createTable(tableDescription);
	}

	Put newMapping() throws IOException
	{
		String key;
		if (upperCaseLookup)
			key = "Q4";
		else
			key = "q4";
		
		Put mapping = new Put(Bytes.toBytes(dataTableName));
		mapping.add(Bytes.toBytes(lookupTableMappingColumnFamilyName),
					 Bytes.toBytes(key),
					 Bytes.toBytes(columnFamilyName + ":q4"));

		return mapping;
	}
}

class HBaseCreateTables 
{
	
	public static void main(String[] args) throws Exception
	{
		// Suppress ZK info level traces
        Logger.getRootLogger().setLevel(Level.ERROR);
        
        System.out.println("Create table 'gphbase_test', with 100 rows per split");
        HBaseCreateTable createTable = 
				new HBaseCreateTable("gphbase_test", 100, false, false);
		createTable.go();
		
		System.out.println("Create table 'gphbase_test_null', with 5 rows per split and null values");
        createTable = 
				new HBaseCreateTable("gphbase_test_null", 5, true, false);
		createTable.go();
		
		System.out.println("Create table 'gphbase_test_upper', with 2 rows per split and key in upper case in lookup table");
        createTable = 
				new HBaseCreateTable("gphbase_test_upper", 2, true, true);
		createTable.go();
		
		System.out.println("Create table 'gphbase_test_integer_rowkey', with integer rowkey and 50 rows per split");
        createTable = 
				new HBaseCreateTable("gphbase_test_integer_rowkey", 50, false, false);
        createTable.setRowKeyPrefix("");
		createTable.go();
       
	}
}
