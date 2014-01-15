import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


enum HbaseLookupCommands 
{
	CREATE_TABLE,
	DROP_TABLE,
	DISABLE_TABLE,
	ENABLE_TABLE,
	REMOVE_CF,
	ADD_CF,
	TRUNCATE_TABLE;

	@Override
	public String toString() {
		//lower case and replace _ with -
		String s = super.toString();
		return s.toLowerCase().replace('_', '-');
	}
};

/*
 * Helper class for testing gphbase protocol.
 * The class will change the lookup table according to command.
 */
class HBaseChangeLookupTable
{
	Configuration config;
	HBaseAdmin admin;
	
	final static String lookupTableName = "pxflookup";
	final static String lookupCfName = "mapping";

	void printStep(String desc)
	{
		System.out.println(desc);
	}

	HBaseChangeLookupTable() throws IOException
	{
		config = HBaseConfiguration.create();
		admin = new HBaseAdmin(config);
	}

	boolean tableExists() throws IOException
	{
		return admin.isTableAvailable(lookupTableName);
	}

	void disableTable() throws IOException
	{
		printStep("disable table");
		if (!tableExists())
			throw new IOException("table " + lookupTableName + " does not exist");
		if (admin.isTableDisabled(lookupTableName))
			return;
		admin.disableTable(lookupTableName);
	}
	
	void enableTable() throws IOException
	{
		printStep("enable table");
		if (!tableExists())
			throw new IOException("table " + lookupTableName + " does not exist");
		if (admin.isTableEnabled(lookupTableName))
			return;
		admin.enableTable(lookupTableName);
	}
	
	void createTable() throws IOException
	{
		if (tableExists())
			throw new IOException("table " + lookupTableName + " already exists");
		
		printStep("create table");	
		HTableDescriptor tableDescription = new HTableDescriptor(TableName.valueOf(lookupTableName));
		tableDescription.addFamily(new HColumnDescriptor(lookupCfName));

		admin.createTable(tableDescription);
	}
	
	void dropTable() throws IOException
	{
		disableTable();
		printStep("drop table");
		admin.deleteTable(lookupTableName);
	}

	void truncateTable() throws IOException
	{
		if (!tableExists())
			throw new IOException("table " + lookupTableName + " does not exist");
		printStep("truncate table");
		HTableDescriptor tableDescription = admin.getTableDescriptor(Bytes.toBytes(lookupTableName));
		dropTable();
		printStep("create table");
		admin.createTable(tableDescription);
	}
	
	void addCf(String columnName) throws IOException
	{
		HColumnDescriptor column = new HColumnDescriptor(Bytes.toBytes(columnName));
		disableTable();
		printStep("add column name " + columnName);
		admin.addColumn(lookupTableName, column);
		enableTable();
	}
	
	void removeCf(String columnName) throws IOException
	{
		disableTable();
		printStep("remove column name " + columnName);
		admin.deleteColumn(lookupTableName, columnName);
		enableTable();
	}
	

	public static void main(String[] args) throws IOException
	{
		// Suppress ZK info level traces
        Logger.getRootLogger().setLevel(Level.ERROR);

        String options = "(options: " +
        		"\n\t'create-table'" +
        		"\n\t'drop-table'" +
        		"\n\t'disable-table'" +
        		"\n\t'enable-table'" +
        		"\n\t'remove-cf'" +
        		"\n\t'add-cf'" +
        		"\n\t'truncate-table'";
        
        if (args.length < 1) 
        {
        	throw new IOException("No command to perform!\n" + options);
        }
        
        String command = args[0];              
        System.out.println("Command: " + Arrays.toString(args));
        
        HBaseChangeLookupTable runner = new HBaseChangeLookupTable();
        
        if (command.compareTo(HbaseLookupCommands.CREATE_TABLE.toString()) == 0)
        	runner.createTable();
        else if (command.compareTo(HbaseLookupCommands.DROP_TABLE.toString()) == 0)
        	runner.dropTable();
        else if (command.compareTo(HbaseLookupCommands.DISABLE_TABLE.toString()) == 0)
        	runner.disableTable();
        else if (command.compareTo(HbaseLookupCommands.ENABLE_TABLE.toString()) == 0)
        	runner.enableTable();
        else if (command.compareTo(HbaseLookupCommands.REMOVE_CF.toString()) == 0)
        		runner.removeCf(args.length == 2 ? args[1] : lookupCfName);
        else if (command.compareTo(HbaseLookupCommands.ADD_CF.toString()) == 0)
        	runner.addCf(args.length == 2 ? args[1] : lookupCfName);
        else if (command.compareTo(HbaseLookupCommands.TRUNCATE_TABLE.toString()) == 0)
        	runner.truncateTable();
        else 
        	throw new IOException("Unkown command " + command + ".\n" + options);
   			
	}
}
