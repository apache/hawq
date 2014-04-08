package com.pxf.tests.fixtures;

import java.io.File;

import com.pivotal.pxfauto.infra.cluster.Cluster;
import com.pivotal.pxfauto.infra.hive.Hive;
import com.pivotal.pxfauto.infra.structures.tables.hive.HiveTable;
import com.pivotal.pxfauto.infra.structures.tables.utils.TableFactory;
import com.pivotal.pxfauto.infra.utils.jsystem.report.ReportUtils;
import com.pxf.tests.basic.PxfHiveRegression;

/**
 * Preparing the system for hive tests. Each Hive using test case should use this Fixture by using
 * the setFixture method.
 * 
 * @see PxfHiveRegression
 */
public class PxfHiveFixture extends BasicFixture {

	private Cluster sc;
	private Hive hive;

	// paths to data resources
	public final static String HIVE_SMALL_DATA_FILE_PATH = new File("regression/resources/hive_small_data.txt").getAbsolutePath();
	public final static String HIVE_TYPES_DATA_FILE_PATH = new File("regression/resources/hive_types.txt").getAbsolutePath();
	public final static String HIVE_COLLECTIONS_DATA_FILE_PATH = new File("regression/resources/hive_collections.txt").getAbsolutePath();

	public static HiveTable hiveSmallDataTable = null;
	public static HiveTable hiveTypesTable = null;
	public static HiveTable hiveSequenceTable = null;
	public static HiveTable hiveRcTable = null;

	/**
	 * Will be called when entering to the fixture.
	 */
	@Override
	protected void setUp() throws Exception {
		super.setUp();

		startFixtureLevel();

		// get cluster object from sut
		sc = (Cluster) system.getSystemObject("cluster");

		// start Hive server for Hive JDBC requests
		sc.startHiveServer();

		// get hive object from sut
		hive = (Hive) system.getSystemObject("hive");

		createRegressionHiveTablesAndLoadData();

		stopFixtureLevel();
	}

	/**
	 * Clean up for the Fixture. This method will be called when pulling out from this Fixture to
	 * the Parent Fixture.
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();

		startFixtureLevel();

		// close hive connection
		hive.close();

		// stop hive server
		sc.stopHiveServer();

		stopFixtureLevel();
	}

	/**
	 * Creates required Hive Tables for regressions and load data to it
	 * 
	 * @throws Exception
	 */
	private void createRegressionHiveTablesAndLoadData() throws Exception {

		ReportUtils.startLevel(report, getClass(), "Create Small Data Hive Table and load data");

		// small data
		hiveSmallDataTable = TableFactory.getHiveByRowCommaTable("reg_txt", new String[] {
				"s1 string",
				"s2 string",
				"n1 int",
				"d1 double" });

		hive.createTableAndVerify(hiveSmallDataTable);
		hive.loadData(hiveSmallDataTable, HIVE_SMALL_DATA_FILE_PATH);

		ReportUtils.stopLevel(report);

		ReportUtils.startLevel(report, getClass(), "Create Hive Types Hive Table and load data");

		// different hive types
		hiveTypesTable = TableFactory.getHiveByRowCommaTable("hive_types", new String[] {
				"s1 string",
				"s2 string",
				"n1 int",
				"d1 double",
				"dc1 decimal",
				"tm timestamp",
				"f float",
				"bg bigint",
				"b boolean" });

		hive.createTableAndVerify(hiveTypesTable);
		hive.loadData(hiveTypesTable, HIVE_TYPES_DATA_FILE_PATH);

		ReportUtils.stopLevel(report);

		ReportUtils.startLevel(report, getClass(), "Create Sequence Hive Table and load data");

		// sequence file
		hiveSequenceTable = TableFactory.getHiveByRowCommaTable("hive_sequence_table", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveSequenceTable.setStoredAs("SEQUENCEFILE");

		hive.createTableAndVerify(hiveSequenceTable);
		hive.runQuery("insert into table " + hiveSequenceTable.getName() + " SELECT * FROM " + hiveSmallDataTable.getName());

		ReportUtils.stopLevel(report);

		ReportUtils.startLevel(report, getClass(), "Create RC Hive Table and load data");

		// rc file
		hiveRcTable = new HiveTable("hive_rc_table", new String[] {
				"t0 string",
				"t1 string",
				"num1 int",
				"d1 double" });

		hiveRcTable.setStoredAs("RCFILE");

		hive.createTableAndVerify(hiveRcTable);
		hive.runQuery("INSERT INTO TABLE " + hiveRcTable.getName() + " SELECT * FROM " + hiveSmallDataTable.getName());

		report.stopLevel();
	}
}