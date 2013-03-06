import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.ArrayList;

/*
 * Helper class for testing gphbase protocol.
 * The class will drop the table tableName from HBase.
 */
class HBaseDropTable
{
	Configuration config;
	HBaseAdmin admin;
	String tableName;
	
	final String lookupTableName = "gpxflookup";

	void go() throws Exception
	{
		if (!tableExists(tableName))
			throw new Exception("table " + tableName + " does not exist");

		printStep("drop");
		dropTable();

		printStep("lookup cleanup");
		cleanupLookup();

		printStep("leave");
	}

	void printStep(String desc)
	{
		System.out.println(desc);
	}

	HBaseDropTable(String tableName) throws Exception
	{
		this.tableName = tableName;
		config = HBaseConfiguration.create();
		admin = new HBaseAdmin(config);
	}

	boolean tableExists(String tableName) throws IOException
	{
		return admin.isTableAvailable(tableName);
	}

	void dropTable() throws IOException
	{
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}

	void cleanupLookup() throws Exception
	{
		if (!tableExists(lookupTableName))
			throw new Exception("Lookup table does not exist!");

		HTable lookup = new HTable(config, lookupTableName);
		Delete mapping = new Delete(Bytes.toBytes(tableName));
		lookup.delete(mapping);
		lookup.close();
	}
}

class HBaseDropTables
{
	public static void main(String[] args) throws Exception
	{
		// Suppress ZK info level traces
        Logger.getRootLogger().setLevel(Level.ERROR);

        System.out.println("table name to drop is 'gphbase_test'");
		HBaseDropTable dropTable = new HBaseDropTable("gphbase_test");
		dropTable.go();
		
		System.out.println("table name to drop is 'gphbase_test_null'");
		dropTable = new HBaseDropTable("gphbase_test_null");
		dropTable.go();
		
		System.out.println("table name to drop is 'gphbase_test_upper'");
		dropTable = new HBaseDropTable("gphbase_test_upper");
		dropTable.go();
        
	}
}
