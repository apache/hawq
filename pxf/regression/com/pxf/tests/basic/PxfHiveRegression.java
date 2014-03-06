package com.pxf.tests.basic;

import java.io.File;

import org.junit.Test;
import org.postgresql.util.PSQLException;

import com.pivotal.pxfauto.infra.hive.Hive;
import com.pivotal.pxfauto.infra.structures.tables.basic.Table;
import com.pivotal.pxfauto.infra.structures.tables.hive.HiveExternalTable;
import com.pivotal.pxfauto.infra.structures.tables.hive.HiveTable;
import com.pivotal.pxfauto.infra.structures.tables.pxf.ReadableExternalTable;
import com.pivotal.pxfauto.infra.structures.tables.utils.TableFactory;
import com.pivotal.pxfauto.infra.utils.exception.ExceptionUtils;
import com.pivotal.pxfauto.infra.utils.tables.ComparisonUtils;
import com.pxf.tests.fixtures.PxfHiveFixture;
import com.pxf.tests.testcases.PxfTestCase;

/**
 * PXF using Hive data Regression Tests
 */
public class PxfHiveRegression extends PxfTestCase {

	Hive hive;

	HiveTable hiveTable;

	ReadableExternalTable hawqExternalTable;

	public PxfHiveRegression() {
		setFixture(PxfHiveFixture.class);
	}

	/**
	 * Create reg_txt table for all tests to use.
	 */
	@Override
	public void defaultBefore() throws Throwable {

		super.defaultBefore();

		hawq.runQuery("SET optimizer = off");
		
		hive = (Hive) system.getSystemObject("hive");

		hiveTable = TableFactory.getHivebyRowCommaTable("reg_txt", new String[] {
				"s1 string",
				"s2 string",
				"n1 int",
				"d1 double" });

		hive.dropTable(hiveTable, false);

		hive.createTable(hiveTable);

		File resource = new File("regression/resources/hive_small_data.txt");

		hive.loadData(hiveTable, resource.getAbsolutePath());
	}

	/**
	 * Try to query a table that doesn't exist.
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeNoTable() throws Exception {
		
		hiveTable = new HiveTable("no_such_hive_table", null);
		
		hawqExternalTable = TableFactory.getPxfHiveReadableTable("no_such_table", new String[] {
				"t1    text",
				"num1  integer"}, hiveTable);

		hawq.createTableAndVerify(hawqExternalTable);

		try {

			hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		} catch (Exception e) {
			
			ExceptionUtils.validate(report, e, 
					new PSQLException("NoSuchObjectException\\(message:default." + hiveTable.getName() + 
							" table not found\\)", null), true);
		}

	}
	
	/**
	 * Create Hive table with primitive types and PXF it.
	 * 
	 * @throws Exception
	 */
	@Test
	public void primitiveTypes() throws Exception {

		hiveTable = TableFactory.getHivebyRowCommaTable("hive_types", new String[] {
				"s1 string",
				"s2 string",
				"n1 int",
				"d1 double",
				"dc1 decimal",
				"tm timestamp",
				"f float",
				"bg bigint",
				"b boolean" });

		hive.dropTable(hiveTable, false);

		hive.createTable(hiveTable);

		File resource = new File("regression/resources/hive_types.txt");

		hive.loadData(hiveTable, resource.getAbsolutePath());

		hive.queryResults(hiveTable, "SELECT * FROM " + hiveTable.getName() + " ORDER BY s1");

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hawq_types", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"dec1  numeric",
				"tm timestamp",
				"r real",
				"bg bigint",
				"b boolean" }, hiveTable);

		hawq.createTableAndVerify(hawqExternalTable);

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hiveTable, hawqExternalTable, report);
	}

	/**
	 * Create Hive table stored as sequence file and PXF it.
	 * 
	 * @throws Exception
	 */
	@Test
	public void storeAsSequence() throws Exception {

		createSequenceHive();

		hive.runQuery("insert into table " + hiveTable.getName() + " SELECT * FROM reg_txt");

		hive.queryResults(hiveTable, "SELECT * FROM " + hiveTable.getName() + " ORDER BY t0");

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hv_seq", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision" }, hiveTable);

		hawq.createTableAndVerify(hawqExternalTable);

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hiveTable, hawqExternalTable, report);
	}

	/**
	 * Create Hive table stored as RC file and PXF it.
	 * 
	 * @throws Exception
	 */
	@Test
	public void storeAsRCFile() throws Exception {

		createRcFileHive();

		hive.runQuery("INSERT INTO TABLE " + hiveTable.getName() + " SELECT * FROM reg_txt");

		hive.queryResults(hiveTable, "SELECT * FROM " + hiveTable.getName() + " ORDER BY t0");

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hv_rc", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision" }, hiveTable);

		hawq.createTableAndVerify(hawqExternalTable);

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hiveTable, hawqExternalTable, report);
	}

	/**
	 * Create Hive table stored as ORC file and PXF it.
	 * 
	 * @throws Exception
	 */
	@Test
	public void storeAsOrc() throws Exception {

		createOrcFileHive();

		hive.runQuery("insert into table " + hiveTable.getName() + " SELECT * FROM reg_txt");

		hive.queryResults(hiveTable, "SELECT * FROM " + hiveTable.getName() + " ORDER BY t0");

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hv_orc", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision" }, hiveTable);

		hawq.createTableAndVerify(hawqExternalTable);

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hiveTable, hawqExternalTable, report);
	}

	/**
	 * Create Hive table separated to different partitions and PXF it. Also
	 * check pg_class table after ANALYZE.
	 * 
	 * @throws Exception
	 */
	@Test
	public void severalPartitions() throws Exception {

		/**
		 * Create used partition tables
		 */
		createSequenceHive();

		createRcFileHive();

		createOrcFileHive();

		HiveExternalTable hiveTable = TableFactory.getHiveByRowCommaExternalTable("reg_heterogen", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveTable.setPartitionBy("fmt string");

		hive.dropTable(hiveTable, false);
		hive.createTable(hiveTable);

		hive.runQuery("ALTER TABLE " + hiveTable.getName() + " ADD PARTITION (fmt = 'txt') LOCATION 'hdfs:/hive/warehouse/reg_txt'");
		hive.runQuery("ALTER TABLE " + hiveTable.getName() + " ADD PARTITION (fmt = 'rc') LOCATION 'hdfs:/hive/warehouse/reg_rc'");
		hive.runQuery("ALTER TABLE " + hiveTable.getName() + " ADD PARTITION (fmt = 'seq') LOCATION 'hdfs:/hive/warehouse/reg_seq'");
		hive.runQuery("ALTER TABLE " + hiveTable.getName() + " ADD PARTITION (fmt = 'orc') LOCATION 'hdfs:/hive/warehouse/reg_orc'");
		hive.runQuery("ALTER TABLE  " + hiveTable.getName() + " PARTITION (fmt='rc') SET FILEFORMAT RCFILE");
		hive.runQuery("ALTER TABLE  " + hiveTable.getName() + " PARTITION (fmt='seq') SET FILEFORMAT SEQUENCEFILE");
		hive.runQuery("ALTER TABLE  " + hiveTable.getName() + " PARTITION (fmt='orc') SET FILEFORMAT ORC");
		hive.queryResults(hiveTable, "SELECT * FROM " + hiveTable.getName() + " ORDER BY fmt, t0");

		/**
		 * Create HAWQ Table using Hive Hive profile
		 */
		ReadableExternalTable extTableUsingProfile = TableFactory.getPxfHiveReadableTable("hv_heterogen_uainf_profile", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"t3 text" }, hiveTable);

		hawq.createTableAndVerify(extTableUsingProfile);

		/**
		 * Create HAWQ table with not using profiles
		 */
		ReadableExternalTable extTableNoProfile = new ReadableExternalTable("hv_heterogen_no_profile", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"t3 text" }, hiveTable.getName(), "custom");

		extTableNoProfile.setFormatter("pxfwritable_import");
		extTableNoProfile.setFragmenter("com.pivotal.pxf.plugins.hive.HiveDataFragmenter");
		extTableNoProfile.setAccessor("com.pivotal.pxf.plugins.hive.HiveAccessor");
		extTableNoProfile.setResolver("com.pivotal.pxf.plugins.hive.HiveResolver");

		hawq.createTableAndVerify(extTableNoProfile);

		hawq.queryResults(extTableUsingProfile, "SELECT * FROM " + extTableUsingProfile.getName() + " ORDER BY t3, t1");
		hawq.queryResults(extTableNoProfile, "SELECT * FROM " + extTableNoProfile.getName() + " ORDER BY t3, t1");

		ComparisonUtils.compareTables(hiveTable, extTableUsingProfile, report);
		ComparisonUtils.compareTables(hiveTable, extTableNoProfile, report);

		/**
		 * Perform Analyze on two kind of external tables and check suitable
		 * Warnings.
		 */
		hawq.runQueryWithExpectedWarning("ANALYZE " + extTableUsingProfile.getName(), "PXF 'Analyzer' class was not found. Please supply it in the LOCATION clause or use it in a PXF profile in order to run ANALYZE on this table", true);
		hawq.runQueryWithExpectedWarning("ANALYZE " + extTableNoProfile.getName(), "no ANALYZER or PROFILE option in table definition", true);

		/**
		 * Check the default analyze results still exists
		 */
		Table analyzeResultsTable = new Table("pg_class", null);

		hawq.queryResults(analyzeResultsTable, "SELECT relpages, reltuples FROM " + analyzeResultsTable.getName() + " WHERE relname = '" + extTableUsingProfile.getName() + "'");

		Table dataSudoTable = new Table("sudoTable", null);
		dataSudoTable.addRow(new String[] { "1000", "1000000" });

		ComparisonUtils.compareTables(analyzeResultsTable, dataSudoTable, report);
	}

	/**
	 * Create Hive table using collections and PXF it
	 * 
	 * @throws Exception
	 */
	@Test
	public void collectionTypes() throws Exception {

		hiveTable = new HiveTable("reg_collections", new String[] {
				"s1 STRING",
				"f1 FLOAT",
				"a1 ARRAY<STRING>",
				"m1 MAP<STRING,  FLOAT >",
				"sr1 STRUCT<street:STRING,  city:STRING,  state:STRING,  zip:INT >" });

		hiveTable.setFormat("row");
		hiveTable.setDelimiterFieldsBy("\\001");
		hiveTable.setDelimiterCollectionItemsBy("\\002");
		hiveTable.setDelimiterMapKeysBy("\\003");
		hiveTable.setDelimiterLinesBy("\\n");
		hiveTable.setStoredAs("TEXTFILE");

		hive.dropTable(hiveTable, false);

		hive.createTable(hiveTable);

		File resource = new File("regression/resources/hive_collections.txt");

		hive.loadData(hiveTable, resource.getAbsolutePath());

		hive.queryResults(hiveTable, "SELECT * FROM " + hiveTable.getName() + " ORDER BY s1");

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hv_collections", new String[] {
				"t1    text",
				"f1    real",
				"t2    text",
				"t3    text",
				"t4    text",
				"t5    text",
				"f2    real",
				"t6    text",
				"f3    real",
				"t7    text",
				"t8    text",
				"t9    text",
				"num1  integer" }, hiveTable);

		hawq.createTableAndVerify(hawqExternalTable);

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hiveTable, hawqExternalTable, report);
	}

	/**
	 * Check no support in Hive views.
	 * 
	 * @throws Exception
	 */
	@Test
	public void viewNegative() throws Exception {

		hive.runQuery("DROP VIEW reg_txt_view");

		hive.runQuery("CREATE VIEW reg_txt_view AS SELECT s1 FROM reg_txt");

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hv_view", new String[] { "t1 text" }, new HiveTable("reg_txt_view", null));

		hawq.createTableAndVerify(hawqExternalTable);

		try {

			hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		} catch (Exception e) {

			ExceptionUtils.validate(report, e, new PSQLException("PXF doesn't support HIVE views", null), true);
		}
	}

	/**
	 * PXF Hive index table.
	 * 
	 * @throws Exception
	 */
	@Test
	public void indexes() throws Exception {

		hiveTable = new HiveTable("default__reg_txt_reg_txt_index__", null);

		hive.runQuery("DROP INDEX reg_txt_index ON reg_txt");
		hive.runQuery("CREATE INDEX reg_txt_index ON table reg_txt (s1) AS 'COMPACT' WITH DEFERRED REBUILD");
		hive.runQuery("ALTER index reg_txt_index ON reg_txt REBUILD");

		hive.queryResults(hiveTable, "SELECT * FROM " + hiveTable.getName() + " ORDER BY s1");

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hv_index", new String[] {
				"t1 text",
				"t2 text",
				"t3 bigint" }, hiveTable);

		hawq.createTableAndVerify(hawqExternalTable);

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hiveTable, hawqExternalTable, report);
	}

	private void createSequenceHive() throws Exception {

		hiveTable = TableFactory.getHivebyRowCommaTable("reg_seq", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveTable.setStoredAs("SEQUENCEFILE");

		hive.dropTable(hiveTable, false);

		hive.createTable(hiveTable);
	}

	private void createRcFileHive() throws Exception {
		hiveTable = new HiveTable("reg_rc", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveTable.setStoredAs("RCFILE");

		hive.dropTable(hiveTable, false);

		hive.createTable(hiveTable);
	}

	private void createOrcFileHive() throws Exception {
		hiveTable = new HiveTable("reg_orc", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveTable.setStoredAs("ORC");

		hive.dropTable(hiveTable, false);

		hive.createTable(hiveTable);
	}
}