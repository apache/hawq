package com.pxf.tests.basic;

import java.io.IOException;

import jsystem.framework.fixture.FixtureManager;
import jsystem.framework.fixture.RootFixture;

import org.apache.commons.lang.StringEscapeUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.pivotal.parot.components.hive.Hive;
import com.pivotal.parot.structures.tables.basic.Table;
import com.pivotal.parot.structures.tables.hive.HiveExternalTable;
import com.pivotal.parot.structures.tables.hive.HiveTable;
import com.pivotal.parot.structures.tables.pxf.ReadableExternalTable;
import com.pivotal.parot.structures.tables.utils.TableFactory;
import com.pivotal.parot.utils.exception.ExceptionUtils;
import com.pivotal.parot.utils.jsystem.report.ReportUtils;
import com.pivotal.parot.utils.tables.ComparisonUtils;
import com.pxf.tests.fixtures.PxfHiveWalmartFixture;
import com.pxf.tests.testcases.PxfTestCase;

/**
 * Hive Walmart Hive RC connector regression
 */
public class PxfHiveRcRegression extends PxfTestCase {

	// Components used in cases
	Hive hive;
	ReadableExternalTable hawqExternalTable;
	Table comparisonDataTable = new Table("comparisonData", null);

	/**
	 * Required Hive Tables for regression tests
	 */
	public HiveTable hiveSmallDataTable = PxfHiveWalmartFixture.hiveSmallDataTable;
	public HiveTable hiveTypesTable = PxfHiveWalmartFixture.hiveTypesTable;;
	public HiveTable hiveRcTable1 = PxfHiveWalmartFixture.hiveRcTable1;
	public HiveTable hiveRcTableNoSerde = PxfHiveWalmartFixture.hiveRcTableNoSerde;
	public HiveTable hiveRcTypes = PxfHiveWalmartFixture.hiveRcTypes;

	/**
	 * Connects PxfHiveRegression to PxfHiveFixture. The Fixture will run once and than the system
	 * will be in that "Fixture state".
	 */
	public PxfHiveRcRegression() {
		setFixture(PxfHiveWalmartFixture.class);
	}

	/**
	 * Initializations
	 */
	@Override
	public void defaultBefore() throws Throwable {

		super.defaultBefore();

		hive = (Hive) system.getSystemObject("hive");

		comparisonDataTable.loadDataFromFile(PxfHiveWalmartFixture.HIVE_SMALL_DATA_FILE_PATH, ",", 0);

		hiveSmallDataTable = PxfHiveWalmartFixture.hiveSmallDataTable;
		hiveTypesTable = PxfHiveWalmartFixture.hiveTypesTable;;
		hiveRcTable1 = PxfHiveWalmartFixture.hiveRcTable1;
		hiveRcTableNoSerde = PxfHiveWalmartFixture.hiveRcTableNoSerde;
		hiveRcTypes = PxfHiveWalmartFixture.hiveRcTypes;
	}

	@AfterClass
	public static void afterClass() throws Throwable {
		// go to RootFixture - this will cause activation of PxfHiveFixture tearDown()
		FixtureManager.getInstance().goTo(RootFixture.getInstance().getName());
	}

	/**
	 * Create Hive table with all supported types:
	 * 
	 * TEXT -> string <br>
	 * INTEGER -> int<br>
	 * FLOAT8 -> double<br>
	 * NUMERIC -> decimal<br>
	 * TIMESTAMP, TIME -> timestamp<br>
	 * REAL -> float <br>
	 * BIGINT -> bigint<br>
	 * BOOLEAN -> boolean<br>
	 * SMALLINT -> smallint (tinyint is converted to smallint)<br>
	 * BYTEA -> binary <br>
	 * 
	 * @throws Exception
	 */
	@Test
	public void supportedTypesRc() throws Exception {

		hawqExternalTable = TableFactory.getPxfHiveRcReadableTable("hawq_hive_types", new String[] {
				"key    TEXT",
				"t1    TEXT",
				"num1  INTEGER",
				"dub1  FLOAT8",
				"tm TIMESTAMP",
				"r REAL",
				"bg BIGINT",
				"b BOOLEAN",
				"si SMALLINT",
				"ba BYTEA" }, hiveRcTypes, true);

		try {
			hawq.createTableAndVerify(hawqExternalTable);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY key");

		comparisonDataTable.loadDataFromFile(PxfHiveWalmartFixture.HIVE_TYPES_DATA_FILE_PATH, ",", 0);

		ComparisonUtils.compareTables(hawqExternalTable, comparisonDataTable, report);
	}

	/**
	 * use unsupported types for walmart RC connector and check for error
	 * 
	 * @throws Exception
	 */
	@Test
	public void mismatchedTypes() throws Exception {

		hawqExternalTable = TableFactory.getPxfHiveRcReadableTable("hawq_hive_types", new String[] {
				"key    TEXT",
				"t1    TEXT",
				"num1  INTEGER",
				"dub1  FLOAT8",
				"tm TIMESTAMP",
				"r REAL",
				"bg BIGINT",
				"b BOOLEAN",
				"si  INTEGER",
				"ba BYTEA" }, hiveRcTypes, false);

		try {
			hawq.createTableAndVerify(hawqExternalTable);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		try {
			hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY key");
			Assert.fail();
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("com.pivotal.pxf.api.UnsupportedTypeException: Schema mismatch definition: Field si \\(Hive type smallint, HAWQ type INTEGER\\)"), true, true);
		}
	}

	/**
	 * use RC connectors on hive text table and expect error
	 * 
	 * @throws Exception
	 */
	@Test
	public void hiveTextUsingRcConnectors() throws Exception {

		hawqExternalTable = TableFactory.getPxfHiveRcReadableTable("hv_text", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision" }, hiveSmallDataTable, false);

		try {
			hawq.createTableAndVerify(hawqExternalTable);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		try {
			hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");
			Assert.fail();
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("HiveInputFormatFragmenter does not yet support org.apache.hadoop.mapred.TextInputFormat for table - reg_txt. Supported InputFormat is org.apache.hadoop.hive.ql.io.RCFileInputFormat"), true, true);
		}
	}

	/**
	 * use PXF RC connectors to get data from Hive RC table with mentioned ColumnarSerDe serde.
	 * 
	 * @throws Exception
	 */
	@Test
	public void hiveRcTable() throws Exception {

		hawqExternalTable = TableFactory.getPxfHiveRcReadableTable("hv_rc", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision" }, hiveRcTable1, true);

		try {
			hawq.createTableAndVerify(hawqExternalTable);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hawqExternalTable, comparisonDataTable, report);
	}

	/**
	 * use PXF RC connectors to get data from Hive RC table without mentioned serde.
	 * 
	 * @throws Exception
	 */
	@Test
	public void hiveRcTableDefaultSerde() throws Exception {

		hawqExternalTable = TableFactory.getPxfHiveRcReadableTable("hv_rc_default_serde", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision" }, hiveRcTableNoSerde, false);

		try {
			hawq.createTableAndVerify(hawqExternalTable);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(hawqExternalTable, comparisonDataTable, report);
	}

	/**
	 * use PXF RC connectors to get data from Hive partitioned table using specific ColumnarSerDe
	 * serde.
	 * 
	 * @throws Exception
	 */
	@Test
	public void severalRcPartitions() throws Exception {

		HiveExternalTable hiveExternalTable = new HiveExternalTable("reg_heterogen", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveExternalTable.setPartitionBy("fmt string");
		hiveExternalTable.setFormat("ROW");
		hiveExternalTable.setSerde("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe");
		hiveExternalTable.setStoredAs("RCFILE");

		hive.createTableAndVerify(hiveExternalTable);

		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc1') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc2') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc3') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");

		// Create PXF Table for Hive portioned table
		ReadableExternalTable extTableNoProfile = TableFactory.getPxfHiveRcReadableTable("hv_heterogen", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"t3 text" }, hiveExternalTable, true);

		try {
			hawq.createTableAndVerify(extTableNoProfile);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		hawq.queryResults(extTableNoProfile, "SELECT * FROM " + extTableNoProfile.getName() + " ORDER BY t3, t1");

		// pump up the small data to fit the unified data
		comparisonDataTable.loadDataFromFile(PxfHiveWalmartFixture.HIVE_SMALL_DATA_FILE_PATH, ",", 0);
		pumpUpComparisonTableData(3, false);

		ComparisonUtils.compareTables(extTableNoProfile, comparisonDataTable, report);
	}

	/**
	 * use PXF RC connectors to get data from Hive partitioned table using default serde.
	 * 
	 * @throws Exception
	 */
	@Test
	public void severalPartitionsDefaultSerde() throws Exception {

		HiveExternalTable hiveExternalTable = new HiveExternalTable("reg_heterogen", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveExternalTable.setPartitionBy("fmt string");
		hiveExternalTable.setStoredAs("RCFILE");

		hive.createTableAndVerify(hiveExternalTable);

		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc1') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTableNoSerde.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc2') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTableNoSerde.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc3') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTableNoSerde.getName() + "'");

		// Create PXF Table for Hive portioned table
		ReadableExternalTable extTableNoProfile = TableFactory.getPxfHiveRcReadableTable("hv_heterogen_using_profile", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"t3 text" }, hiveExternalTable, false);

		try {
			hawq.createTableAndVerify(extTableNoProfile);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		hawq.queryResults(extTableNoProfile, "SELECT * FROM " + extTableNoProfile.getName() + " ORDER BY t3, t1");

		// pump up the small data to fit the unified data
		comparisonDataTable.loadDataFromFile(PxfHiveWalmartFixture.HIVE_SMALL_DATA_FILE_PATH, ",", 0);
		pumpUpComparisonTableData(3, false);

		ComparisonUtils.compareTables(extTableNoProfile, comparisonDataTable, report);
	}

	/**
	 * Create HAWQ external table without the partition column. check error.
	 * 
	 * @throws Exception
	 */
	@Test
	public void severalRcPartitionsNoPartitonColumInHawq() throws Exception {

		HiveExternalTable hiveExternalTable = new HiveExternalTable("reg_heterogen", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveExternalTable.setPartitionBy("fmt string");
		hiveExternalTable.setFormat("ROW");
		hiveExternalTable.setSerde("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe");
		hiveExternalTable.setStoredAs("RCFILE");

		hive.createTableAndVerify(hiveExternalTable);

		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc1') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc2') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc3') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");

		/**
		 * Create PXF Table using Hive profile
		 */
		ReadableExternalTable extTableNoProfile = TableFactory.getPxfHiveRcReadableTable("hv_heterogen_using_profile", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision" }, hiveExternalTable, true);

		try {
			hawq.createTableAndVerify(extTableNoProfile);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		try {
			hawq.queryResults(extTableNoProfile, "SELECT * FROM " + extTableNoProfile.getName() + " ORDER BY t3, t1");
			Assert.fail();
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("ERROR: column \"t3\" does not exist"), true, true);
		}
	}

	/**
	 * Filter partitions columns on external table directed to hive partitioned table
	 * 
	 * @throws Exception
	 */
	@Test
	public void filterBetweenPartitions() throws Exception {

		HiveExternalTable hiveExternalTable = new HiveExternalTable("reg_heterogen", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveExternalTable.setPartitionBy("fmt string, part string");
		hiveExternalTable.setFormat("ROW");
		hiveExternalTable.setSerde("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe");
		hiveExternalTable.setStoredAs("RCFILE");

		hive.createTableAndVerify(hiveExternalTable);

		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc1', part = 'a') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc2', part = 'b') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc3', part = 'c') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");

		/**
		 * Create PXF Table using Hive profile
		 */
		ReadableExternalTable extTableNoProfile = TableFactory.getPxfHiveRcReadableTable("hv_heterogen_using_filter", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"t3 text",
				"prt text" }, hiveExternalTable, false);

		try {
			hawq.createTableAndVerify(extTableNoProfile);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		hawq.queryResults(extTableNoProfile, "SELECT * FROM " + extTableNoProfile.getName() + " WHERE t3 = 'rc1' AND prt = 'a' ORDER BY t3, t1");

		// pump up the small data to fit the unified data
		comparisonDataTable.loadDataFromFile(PxfHiveWalmartFixture.HIVE_SMALL_DATA_FILE_PATH, ",", 0);
		pumpUpComparisonTableData(1, true);

		ComparisonUtils.compareTables(extTableNoProfile, comparisonDataTable, report);
	}

	/**
	 * Filter none partitions columns on external table directed to hive partitioned table
	 * 
	 * @throws Exception
	 */
	@Test
	public void filterNonePartitions() throws Exception {

		HiveExternalTable hiveExternalTable = new HiveExternalTable("reg_heterogen", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveExternalTable.setPartitionBy("fmt string, part string");
		hiveExternalTable.setFormat("ROW");
		hiveExternalTable.setSerde("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe");
		hiveExternalTable.setStoredAs("RCFILE");

		hive.createTableAndVerify(hiveExternalTable);

		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc1', part = 'a') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc2', part = 'b') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");
		hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc3', part = 'c') LOCATION 'hdfs:/hive/warehouse/" + hiveRcTable1.getName() + "'");

		/**
		 * Create PXF Table using Hive profile
		 */
		ReadableExternalTable extTableNoProfile = TableFactory.getPxfHiveRcReadableTable("hv_heterogen_using_filter", new String[] {
				"t1    text",
				"t2    text",
				"num1  integer",
				"dub1  double precision",
				"t3 text",
				"prt text" }, hiveExternalTable, true);

		try {
			hawq.createTableAndVerify(extTableNoProfile);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		hawq.queryResults(extTableNoProfile, "SELECT * FROM " + extTableNoProfile.getName() + " WHERE num1 > 5 AND dub1 < 12 ORDER BY t3, t1");

		Table dataCompareTable = new Table("dataTable", null);

		// prepare expected data
		dataCompareTable.addRow(new String[] { "row6", "s_11", "6", "11", "rc1", "a" });
		dataCompareTable.addRow(new String[] { "row6", "s_11", "6", "11", "rc2", "b" });
		dataCompareTable.addRow(new String[] { "row6", "s_11", "6", "11", "rc3", "c" });

		ComparisonUtils.compareTables(extTableNoProfile, dataCompareTable, report);
	}

	/**
	 * check none supported Hive types error
	 * 
	 * @throws Exception
	 */
	@Test
	public void noneSupportedHiveTypes() throws Exception {
		HiveTable hiveTable = new HiveTable("reg_collections", new String[] {
				"s1 STRING",
				"f1 FLOAT",
				"a1 ARRAY<STRING>",
				"m1 MAP<STRING,  FLOAT >",
				"sr1 STRUCT<street:STRING,  city:STRING,  state:STRING,  zip:INT >" });

		hiveTable.setFormat("ROW");
		hiveTable.setSerde("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe");
		hiveTable.setStoredAs("RCFILE");

		hive.createTableAndVerify(hiveTable);

		hawqExternalTable = TableFactory.getPxfHiveRcReadableTable("hv_collections", new String[] {
				"t1    text",
				"f1    real",
				"t2    text",
				"t3    text",
				"t4    text" }, hiveTable, false);

		try {
			hawq.createTableAndVerify(hawqExternalTable);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
		}

		try {
			hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");
			Assert.fail();
		} catch (Exception e) {
			String expectedMessage = StringEscapeUtils.escapeHtml("com.pivotal.pxf.api.UnsupportedTypeException: Schema mismatch definition: Field t2 \\(Hive type array<string>, HAWQ type TEXT\\)");
			ExceptionUtils.validate(report, e, new Exception(expectedMessage), true, true);
		}
	}

	/**
	 * Pump up the comparison table data for partitions test case
	 * 
	 * @throws IOException
	 */
	private void pumpUpComparisonTableData(int pumpAmount, boolean useSecondPartition)
			throws IOException {

		ReportUtils.startLevel(report, getClass(), "Pump Up Comparasion Table Data");

		// get original number of line before pump
		int originalNumberOfLines = comparisonDataTable.getData().size();

		// duplicate data in factor of 3
		comparisonDataTable.pumpUpTableData(pumpAmount, true);

		ReportUtils.reportHtml(report, getClass(), comparisonDataTable.getDataHtml());

		// extra field to add
		String[] arr = { "rc1", "rc2", "rc3" };
		String[] arr2 = { "a", "b", "c" };

		int lastIndex = 0;

		// run over fields to add and add it in batches of
		// "originalNumberOfLines"
		for (int i = 0; i < pumpAmount; i++) {
			for (int j = lastIndex; j < (lastIndex + originalNumberOfLines); j++) {
				comparisonDataTable.getData().get(j).add(arr[i]);

				if (useSecondPartition) {
					comparisonDataTable.getData().get(j).add(arr2[i]);
				}
			}

			lastIndex += originalNumberOfLines;
		}

		ReportUtils.stopLevel(report);
	}
}