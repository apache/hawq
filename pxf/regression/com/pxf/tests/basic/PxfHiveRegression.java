package com.pxf.tests.basic;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

import jsystem.framework.fixture.FixtureManager;
import jsystem.framework.fixture.RootFixture;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import com.pivotal.parot.components.hive.Hive;
import com.pivotal.parot.structures.tables.basic.Table;
import com.pivotal.parot.structures.tables.hive.HiveExternalTable;
import com.pivotal.parot.structures.tables.hive.HiveTable;
import com.pivotal.parot.structures.tables.pxf.ReadableExternalTable;
import com.pivotal.parot.structures.tables.utils.TableFactory;
import com.pivotal.parot.utils.exception.ExceptionUtils;
import com.pivotal.parot.utils.jsystem.report.ReportUtils;
import com.pivotal.parot.utils.tables.ComparisonUtils;
import com.pivotal.pxf.service.io.GPDBWritable.TypeMismatchException;
import com.pxf.tests.fixtures.PxfHiveFixture;
import com.pxf.tests.testcases.PxfTestCase;

/**
 * PXF using Hive data Regression Tests
 */
public class PxfHiveRegression extends PxfTestCase {

	Hive hive;
	ReadableExternalTable hawqExternalTable;
	Table comparisonDataTable = new Table("comparisonData", null);

	/**
	 * Required Hive Tables for regression tests
	 */
	public HiveTable hiveSmallDataTable = PxfHiveFixture.hiveSmallDataTable;
	public HiveTable hiveTypesTable = PxfHiveFixture.hiveTypesTable;;
	public HiveTable hiveSequenceTable = PxfHiveFixture.hiveSequenceTable;;
	public HiveTable hiveRcTable = PxfHiveFixture.hiveRcTable;;

	/**
	 * Connects PxfHiveRegression to PxfHiveFixture. The Fixture will run once and than the system
	 * will be in that "Fixture state".
	 */
	public PxfHiveRegression() {
		setFixture(PxfHiveFixture.class);
	}

	/**
	 * Initializations
	 */
	@Before
	@Override
	public void defaultBefore() throws Throwable {

		super.defaultBefore();

		hive = (Hive) system.getSystemObject("hive");

		comparisonDataTable.loadDataFromFile(PxfHiveFixture.HIVE_SMALL_DATA_FILE_PATH, ",", 0);

		hiveSmallDataTable = PxfHiveFixture.hiveSmallDataTable;
		hiveTypesTable = PxfHiveFixture.hiveTypesTable;;
		hiveSequenceTable = PxfHiveFixture.hiveSequenceTable;;
		hiveRcTable = PxfHiveFixture.hiveRcTable;;
	}

	@AfterClass
	public static void afterClass() throws Throwable {

		// go to RootFixture - this will cause activation of PxfHiveFixture tearDown()
		FixtureManager.getInstance().goTo(RootFixture.getInstance().getName());
	}

	/**
	 * Try to query a table that doesn't exist.
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeNoTable() throws Exception {

		HiveTable notExistingTable = new HiveTable("no_such_hive_table", null);

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("no_such_table", new String[] {
				"t1    text",
				"num1  integer" }, notExistingTable);

		hawq.createTableAndVerify(hawqExternalTable);

		try {

			hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		} catch (Exception e) {

			ExceptionUtils.validate(report, e, new PSQLException("NoSuchObjectException\\(message:default." + notExistingTable.getName() + " table not found\\)", null), true);
		}
	}

	/**
	 * Create Hive table with primitive types and PXF it.
	 * 
	 * @throws Exception
	 */
	@Test
	public void primitiveTypes() throws Exception {

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hawq_hive_types", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"dec1  numeric",
				"tm timestamp",
				"r real",
				"bg bigint",
				"b boolean" }, hiveTypesTable);

		hawq.createTableAndVerify(hawqExternalTable);

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		comparisonDataTable.loadDataFromFile(PxfHiveFixture.HIVE_TYPES_DATA_FILE_PATH, ",", 0);

		ComparisonUtils.compareTables(hawqExternalTable, comparisonDataTable, report);
	}

	/**
	 * Create Hive table stored as sequence file and PXF it.
	 * 
	 * @throws Exception
	 */
	@Test
	public void storeAsSequence() throws Exception {

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hv_seq", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision" }, hiveSequenceTable);

		hawq.createTableAndVerify(hawqExternalTable);
		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hawqExternalTable, comparisonDataTable, report);
	}

	/**
	 * Create Hive table stored as RC file and PXF it.
	 * 
	 * @throws Exception
	 */
	@Test
	public void storeAsRCFile() throws Exception {

		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hv_rc", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision" }, hiveRcTable);

		hawq.createTableAndVerify(hawqExternalTable);

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hawqExternalTable, comparisonDataTable, report);
	}

	/**
	 * Create Hive table separated to different partitions (text, RC and Sequence) and PXF it. Also
	 * check pg_class table after ANALYZE.
	 * 
	 * @throws Exception
	 */
	@Test
	public void severalPartitions() throws Exception {

		HiveExternalTable hiveExternalTable = TableFactory.getHiveByRowCommaExternalTable("reg_heterogen", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveExternalTable.setPartitionBy("fmt string");

		hive.createTableAndVerify(hiveExternalTable);

		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'txt') LOCATION 'hdfs:/hive/warehouse/" + hiveSmallDataTable.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'seq') LOCATION 'hdfs:/hive/warehouse/" + hiveSequenceTable.getName() + "'");
		hive.runQuery("ALTER TABLE  " + hiveExternalTable.getName() + " PARTITION (fmt='rc') SET FILEFORMAT RCFILE");
		hive.runQuery("ALTER TABLE  " + hiveExternalTable.getName() + " PARTITION (fmt='seq') SET FILEFORMAT SEQUENCEFILE");

		/**
		 * Create PXF Table using Hive profile
		 */
		ReadableExternalTable extTableUsingProfile = TableFactory.getPxfHiveReadableTable("hv_heterogen_using_profile", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"t3 text" }, hiveExternalTable);

		hawq.createTableAndVerify(extTableUsingProfile);

		/**
		 * Create HAWQ table with not using profiles
		 */
		ReadableExternalTable extTableNoProfile = new ReadableExternalTable("hv_heterogen_no_profile", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"t3 text" }, hiveExternalTable.getName(), "custom");

		extTableNoProfile.setFormatter("pxfwritable_import");
		extTableNoProfile.setFragmenter("com.pivotal.pxf.plugins.hive.HiveDataFragmenter");
		extTableNoProfile.setAccessor("com.pivotal.pxf.plugins.hive.HiveAccessor");
		extTableNoProfile.setResolver("com.pivotal.pxf.plugins.hive.HiveResolver");

		hawq.createTableAndVerify(extTableNoProfile);

		hawq.queryResults(extTableUsingProfile, "SELECT * FROM " + extTableUsingProfile.getName() + " ORDER BY t3, t1");
		hawq.queryResults(extTableNoProfile, "SELECT * FROM " + extTableNoProfile.getName() + " ORDER BY t3, t1");

		// pump up the small data to fit the unified data
		pumpUpComparisonTableData();

		ComparisonUtils.compareTables(extTableUsingProfile, comparisonDataTable, report);
		ComparisonUtils.compareTables(extTableNoProfile, comparisonDataTable, report);

		/**
		 * Perform Analyze on two kind of external tables and check suitable Warnings.
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
	 * Pump up the comparison table data for partitions test case
	 * 
	 * @throws IOException
	 */
	private void pumpUpComparisonTableData() throws IOException {

		ReportUtils.startLevel(report, getClass(), "Pump Up Comparasion Table Data");

		// get original number of line before pump
		int originalNumberOfLines = comparisonDataTable.getData().size();

		// duplicate data in factor of 3
		comparisonDataTable.pumpUpTableData(3, true);

		ReportUtils.reportHtml(report, getClass(), comparisonDataTable.getDataHtml());

		// extra field to add
		String[] arr = { "rc", "seq", "txt" };

		int lastIndex = 0;

		// run over fields to add and add it in batches of "originalNumberOfLines"
		for (int i = 0; i < arr.length; i++) {
			for (int j = lastIndex; j < (lastIndex + originalNumberOfLines); j++) {
				comparisonDataTable.getData().get(j).add(arr[i]);
			}

			lastIndex += originalNumberOfLines;
		}

		ReportUtils.stopLevel(report);
	}

	/**
	 * Create Hive table using collections and PXF it
	 * 
	 * @throws Exception
	 */
	@Test
	public void collectionTypes() throws Exception {

		HiveTable hiveCollectionTable = new HiveTable("reg_collections", new String[] {
				"s1 STRING",
				"f1 FLOAT",
				"a1 ARRAY<STRING>",
				"m1 MAP<STRING,  FLOAT >",
				"sr1 STRUCT<street:STRING,  city:STRING,  state:STRING,  zip:INT >" });

		hiveCollectionTable.setFormat("row");
		hiveCollectionTable.setDelimiterFieldsBy("\\001");
		hiveCollectionTable.setDelimiterCollectionItemsBy("\\002");
		hiveCollectionTable.setDelimiterMapKeysBy("\\003");
		hiveCollectionTable.setDelimiterLinesBy("\\n");
		hiveCollectionTable.setStoredAs("TEXTFILE");

		hive.createTableAndVerify(hiveCollectionTable);
		hive.loadData(hiveCollectionTable, PxfHiveFixture.HIVE_COLLECTIONS_DATA_FILE_PATH);
		hive.queryResults(hiveCollectionTable, "SELECT * FROM " + hiveCollectionTable.getName() + " ORDER BY s1");

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
				"num1  integer" }, hiveCollectionTable);

		hawq.createTableAndVerify(hawqExternalTable);

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hiveCollectionTable, hawqExternalTable, report);
	}
	
	/**
	 * Test demonstrates how missing items in a hive array can be discovered.
	 * In this case we have an <int> Hive array which is mapped to three integer fields in Hawq
	 * When one of the records in the Hive table has only two integers in the array we issue
	 * an exception from Hawq. This is fine, only that it happens due to the fact that the next
	 * field after the <int> array is a string. If the next field were of the same type - an int,
	 * then an exception would not be thrown and we would have got a null on the last field - 
	 * This is a weakness of the current implementation  - demonstrated in the next test
	 * 
	 * @throws Exception
	 */
	@Test
	public void arrayTypeIncompleteDiscovered() throws Exception {
		HiveTable hiveCollectionTable = new HiveTable("hive_array", new String[] {
				"s1 STRING",
				"a1 ARRAY<INT>",
				"s2 STRING" });
		
		hiveCollectionTable.setFormat("row");
		hiveCollectionTable.setDelimiterFieldsBy(",");
		hiveCollectionTable.setDelimiterCollectionItemsBy("|");
		hiveCollectionTable.setDelimiterLinesBy("\\n");
		hiveCollectionTable.setStoredAs("TEXTFILE");
		
		hive.createTableAndVerify(hiveCollectionTable);
		hive.loadData(hiveCollectionTable, PxfHiveFixture.HIVE_NULLS1_DATA_FILE_PATH);
		hive.queryResults(hiveCollectionTable, "SELECT * FROM " + hiveCollectionTable.getName() + " ORDER BY s1");
		
		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hawq_array_tbl", new String[] {
				"t1    text",
				"num1  integer",
				"num2  integer",
				"num3  integer",
				"t2    text",
				}, hiveCollectionTable);
		hawq.createTableAndVerify(hawqExternalTable);
		
		/* Data file:
		 * one,11|12|13,one
         * two,1|23,two
         * 
         * The second record has the fourth field missing ( the third field in the array)
         * We can discover this only because the next Hive field is text and not int
		 */
		try
		{
			hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");
		} catch (org.postgresql.util.PSQLException e) {
			String cur = e.getMessage();
 			boolean has = cur.contains("For field num3 schema requires type INTEGER but input record has type TEXT");
			Assert.assertTrue(has);
			return;
		}
		Assert.assertTrue("queryResults() in arrayTypeIncompleteDiscovered should have raised an exception",false);
	}
	
	/**
	 * Test demonstrates a case where missing items in a hive array ARE NOT discovered.
	 * In this case we have an <int> Hive array which is mapped to three integer fields in Hawq
	 * When one of the records in the Hive table has only two integers in the array, we DO NOT issue
	 * an exception from Hawq. It happens due to the fact that the next field after the <int> array 
	 * is of the same type - an int. Instead of  an exception we get a null on the last field - 
	 * This is a weakness of the current implementation.
	 * 
	 * @throws Exception
	 */
	@Test
	public void arrayTypeIncompleteNotFound() throws Exception {
		HiveTable hiveCollectionTable = new HiveTable("hive_array", new String[] {
				"s1 STRING",
				"a1 ARRAY<INT>",
				"a2 INT" });
		
		hiveCollectionTable.setFormat("row");
		hiveCollectionTable.setDelimiterFieldsBy(",");
		hiveCollectionTable.setDelimiterCollectionItemsBy("|");
		hiveCollectionTable.setDelimiterLinesBy("\\n");
		hiveCollectionTable.setStoredAs("TEXTFILE");
		
		hive.createTableAndVerify(hiveCollectionTable);
		hive.loadData(hiveCollectionTable, PxfHiveFixture.HIVE_NULLS2_DATA_FILE_PATH);
		hive.queryResults(hiveCollectionTable, "SELECT * FROM " + hiveCollectionTable.getName() + " ORDER BY s1");
		
		hawqExternalTable = TableFactory.getPxfHiveReadableTable("hawq_array_tbl", new String[] {
				"t1    text",
				"num1  integer",
				"num2  integer",
				"num3  integer",
				"num4  integer",
				}, hiveCollectionTable);
		hawq.createTableAndVerify(hawqExternalTable);
		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");
		
		/* Data file:
		 * one,11|12|13,14
         * two,1|23,24
         * 
         * The second record has a NULL in the fifth field instead of the fourth field
         * This is the weakness in the current implementation that this test exposes
		 */
		
		List<List<String>> data = hawqExternalTable.getData();
		List<String> faulty = data.get(1);
		int size = faulty.size();
		Assert.assertEquals(size, 5);
		Assert.assertEquals(faulty.get(3), "24");
		Assert.assertNull(faulty.get(4));
	}

	/**
	 * Will run after all tests in the class ran. Will call to PxfHiveFixture tear down.
	 * 
	 * @throws Throwable
	 */

	@Test
	public void primitiveTypesDeprecatedClasses() throws Exception {

		comparisonDataTable.loadDataFromFile(PxfHiveFixture.HIVE_TYPES_DATA_FILE_PATH, ",", 0);

		hawqExternalTable = new ReadableExternalTable("hive_types", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"dec1  numeric",
				"tm timestamp",
				"r real",
				"bg bigint",
				"b boolean" }, hiveTypesTable.getName(), "CUSTOM");

		hawqExternalTable.setFragmenter("HiveDataFragmenter");
		hawqExternalTable.setAccessor("HiveAccessor");
		hawqExternalTable.setResolver("HiveResolver");
		hawqExternalTable.setFormatter("pxfwritable_import");

		try {
			hawq.createTableAndVerify(hawqExternalTable);
			Assert.fail("A SQLWarning should have been thrown");
		} catch (SQLWarning warnings) {
			SQLWarning warning = warnings;
			assertUseIsDeprecated("HiveDataFragmenter", warning);
			warning = warning.getNextWarning();
			assertUseIsDeprecated("HiveAccessor", warning);
			warning = warning.getNextWarning();
			assertUseIsDeprecated("HiveResolver", warning);
			warning = warning.getNextWarning();
			Assert.assertNull(warning);
		}

		// TODO once jsystem-infra supports throwing warnings from queryResults check warnings are
		// also printed here
		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hawqExternalTable, comparisonDataTable, report);
	}

	private void assertUseIsDeprecated(String classname, SQLWarning warning) {
		Assert.assertEquals("Use of " + classname + " is deprecated and it will be removed on the next major version", warning.getMessage());
	}
}