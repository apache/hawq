package com.pxf.tests.fixtures;

import com.pivotal.parot.components.cluster.PhdCluster;
import com.pivotal.parot.components.hive.Hive;
import com.pivotal.parot.structures.tables.hive.HiveTable;
import com.pivotal.parot.structures.tables.utils.TableFactory;
import com.pivotal.parot.utils.jsystem.report.ReportUtils;
import jsystem.framework.report.Reporter;

import java.io.File;


/**
 * Preparing the system for Hive Text Regression
 *
 * @see com.pxf.tests.basic.PxfHiveRegression
 */
public class PxfHiveTextFixture extends BasicFixture {

    private PhdCluster sc;
    private Hive hive;

    // paths to data resources
    public final static String HIVE_SMALL_DATA_FILE_PATH = new File("resources/hive_small_data.txt").getAbsolutePath();
    public final static String HIVE_TYPES_DATA_FILE_PATH = new File("resources/hive_types_walmart.txt").getAbsolutePath();

    public static HiveTable hiveSmallDataTable = null;
    public static HiveTable hiveTypesTable = null;
    public static HiveTable hiveTextTypes = null;
    public static HiveTable hiveRcTable = null;

    public static HiveTable hiveTextTable = null;

    /**
     * Will be called when entering to the fixture.
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        startFixtureLevel();

        // get cluster object from sut
        sc = (PhdCluster) system.getSystemObject("cluster");

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

        try {
            // stop hive server
            sc.stopHiveServer();
        } catch (Exception e) {
            ReportUtils.report(report, getClass(), "Problem stopping Hive Server", Reporter.WARNING);
        }

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
                "d1 double"});

        hive.createTableAndVerify(hiveSmallDataTable);
        hive.loadData(hiveSmallDataTable, HIVE_SMALL_DATA_FILE_PATH);

        ReportUtils.stopLevel(report);

        // create hive table, load data from file to hive table
        ReportUtils.startLevel(report, getClass(), "Create Hive Types Hive Table and load data");

        hiveTypesTable = TableFactory.getHiveByRowCommaTable("hive_types", new String[] {
                "key string",
                "s1 string",
                "n1 int",
                "d1 double",
                "tm timestamp",
                "f float",
                "bg bigint",
                "b boolean",
                "si smallint",
                "bin binary"});

        hive.createTableAndVerify(hiveTypesTable);
        hive.loadData(hiveTypesTable, HIVE_TYPES_DATA_FILE_PATH);

        ReportUtils.stopLevel(report);

        // create hive Text table and load data from "hive_types" table to it
        ReportUtils.startLevel(report, getClass(), "Create Text Hive Types Table and load data");

        hiveTextTypes = new HiveTable("text_hive_types", new String[] {
                "key string",
                "s1 string",
                "n1 int",
                "d1 double",
                "tm timestamp",
                "f float",
                "bg bigint",
                "b boolean",
                "si smallint",
                "bin binary"});

        hive.createTableAndVerify(hiveTextTypes);
        hive.runQuery("INSERT INTO TABLE " + hiveTextTypes.getName() + " SELECT * FROM " + hiveTypesTable.getName());

        report.stopLevel();

        hiveTextTable = createTextTableAndLoadData("hive_text_table");
        hiveRcTable = createRcTableAndLoadData("hive_rc_table");
    }

    /**
     * Create Hive Text table and load small data to it
     *
     * @param tableName for new Hive Text table
     * @return {@link HiveTable} object
     * @throws Exception
     */
    private HiveTable createTextTableAndLoadData(String tableName)
            throws Exception {
        ReportUtils.startLevel(report, getClass(), "Create Text Hive Table and load data");

        HiveTable hiveTextTable = new HiveTable(tableName, new String[] {
                "t0 string",
                "t1 string",
                "num1 int",
                "d1 double"});

        hive.createTableAndVerify(hiveTextTable);

        // load data from small data text file
        hive.runQuery("INSERT INTO TABLE " + hiveTextTable.getName() + " SELECT * FROM " + hiveSmallDataTable.getName());

        report.stopLevel();

        return hiveTextTable;
    }

    /**
     * Create Hive RC table and load small data to it
     *
     * @param tableName for new Hive RC table
     * @return {@link HiveTable} object
     * @throws Exception
     */
    private HiveTable createRcTableAndLoadData(String tableName)
            throws Exception {
        ReportUtils.startLevel(report, getClass(), "Create RC Hive Table and load data");

        HiveTable hiveRcTable = new HiveTable(tableName, new String[] {
                "t0 string",
                "t1 string",
                "num1 int",
                "d1 double" });

        // set RCFILE format for storage
        hiveRcTable.setStoredAs("RCFILE");

        hive.createTableAndVerify(hiveRcTable);

        // load data from small data text file
        hive.runQuery("INSERT INTO TABLE " + hiveRcTable.getName() + " SELECT * FROM " + hiveSmallDataTable.getName());

        report.stopLevel();

        return hiveRcTable;
    }
}
