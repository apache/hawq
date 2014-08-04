package com.pxf.tests.basic;

import com.pivotal.parot.components.hive.Hive;
import com.pivotal.parot.structures.tables.basic.Table;
import com.pivotal.parot.structures.tables.hive.HiveExternalTable;
import com.pivotal.parot.structures.tables.hive.HiveTable;
import com.pivotal.parot.structures.tables.pxf.ReadableExternalTable;
import com.pivotal.parot.structures.tables.utils.TableFactory;
import com.pivotal.parot.utils.exception.ExceptionUtils;
import com.pivotal.parot.utils.jsystem.report.ReportUtils;
import com.pivotal.parot.utils.tables.ComparisonUtils;
import com.pxf.tests.fixtures.PxfHiveRcFixture;
import com.pxf.tests.fixtures.PxfHiveTextFixture;
import com.pxf.tests.testcases.PxfTestCase;
import jsystem.framework.fixture.FixtureManager;
import jsystem.framework.fixture.RootFixture;
import org.apache.commons.lang.StringEscapeUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Hive Text connector regression
 */
public class PxfHiveTextRegression extends PxfTestCase {

    // Components used in cases
    Hive hive;
    ReadableExternalTable hawqExternalTable;
    Table comparisonDataTable = new Table("comparisonData", null);

    /**
     * Required Hive Tables for regression tests
     */
    public HiveTable hiveSmallDataTable = PxfHiveTextFixture.hiveSmallDataTable;
    public HiveTable hiveTypesTable = PxfHiveTextFixture.hiveTypesTable;
    public HiveTable hiveRcTable = PxfHiveTextFixture.hiveRcTable;

    public HiveTable hiveTextTable = PxfHiveTextFixture.hiveTextTable;
    public HiveTable hiveTextTypes = PxfHiveTextFixture.hiveTextTypes;

    /**
     * Connects PxfHiveTextRegression to PxfHiveTextFixture. The Fixture will run once and than the system
     * will be in that "Fixture state".
     */
    public PxfHiveTextRegression() {
        setFixture(PxfHiveTextFixture.class);
    }

    /**
     * Initializations
     */
    @Override
    public void defaultBefore() throws Throwable {

        super.defaultBefore();

        hive = (Hive) system.getSystemObject("hive");

        comparisonDataTable.loadDataFromFile(PxfHiveTextFixture.HIVE_SMALL_DATA_FILE_PATH, ",", 0);

        hiveSmallDataTable = PxfHiveTextFixture.hiveSmallDataTable;
        hiveTypesTable = PxfHiveTextFixture.hiveTypesTable;

        hiveTextTable = PxfHiveTextFixture.hiveTextTable;
        hiveTextTypes = PxfHiveTextFixture.hiveTextTypes;
    }

    @AfterClass
    public static void afterClass() throws Throwable {
        // go to RootFixture - this will cause activation of PxfHiveFixture tearDown()
        FixtureManager.getInstance().goTo(RootFixture.getInstance().getName());
    }

    /**
     * Create Hive table with all supported types:
     * <p/>
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
    public void supportedTypesText() throws Exception {

        hawqExternalTable = TableFactory.getPxfHiveTextReadableTable("hawq_hive_types", new String[] {
                "key    TEXT",
                "t1    TEXT",
                "num1  INTEGER",
                "dub1  FLOAT8",
                "tm TIMESTAMP",
                "r REAL",
                "bg BIGINT",
                "b BOOLEAN",
                "si SMALLINT",
                "ba BYTEA"}, hiveTextTypes, true);

        try {
            hawq.createTableAndVerify(hawqExternalTable);
        } catch (Exception e) {
            ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
        }

        hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY key");

        comparisonDataTable.loadDataFromFile(PxfHiveTextFixture.HIVE_TYPES_DATA_FILE_PATH, ",", 0);

        ComparisonUtils.compareTables(hawqExternalTable, comparisonDataTable, report);
    }

    /**
     * use unsupported types for Text connector and check for error
     *
     * @throws Exception
     */
    @Test
    public void mismatchedTypes() throws Exception {

        hawqExternalTable = TableFactory.getPxfHiveTextReadableTable("hawq_hive_types", new String[] {
                "key    TEXT",
                "t1    TEXT",
                "num1  INTEGER",
                "dub1  FLOAT8",
                "tm TIMESTAMP",
                "r REAL",
                "bg BIGINT",
                "b BOOLEAN",
                "si  INTEGER",
                "ba BYTEA"}, hiveTextTypes, false);

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
     * use Text connectors on hive rc table and expect error
     *
     * @throws Exception
     */
    @Test
    public void hiveRcUsingTextConnectors() throws Exception {

        hawqExternalTable = TableFactory.getPxfHiveTextReadableTable("hv_rc", new String[] {
                "t1    text",
                "t2    text",
                "num1  integer",
                "dub1  double precision" }, hiveRcTable, false);

        try {
            hawq.createTableAndVerify(hawqExternalTable);
        } catch (Exception e) {
            ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
        }

        try {
            hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");
            Assert.fail();
        } catch (Exception e) {
            ExceptionUtils.validate(report, e, new Exception("LAZY_BINARY_COLUMNAR_SERDE serializer isn't supported by com.pivotal.pxf.plugins.hive.HiveLineBreakAccessor"), true, true);
        }
    }

    /**
     * use PXF Text connectors to get data from Hive Text table
     *
     * @throws Exception
     */
    @Test
    public void hiveTextTable() throws Exception {

        hawqExternalTable = TableFactory.getPxfHiveTextReadableTable("hv_txt", new String[] {
                "t1    text",
                "t2    text",
                "num1  integer",
                "dub1  double precision"}, hiveTextTable, true);

        try {
            hawq.createTableAndVerify(hawqExternalTable);
        } catch (Exception e) {
            ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
        }

        hawq.queryResults(hawqExternalTable, "SELECT * FROM " + hawqExternalTable.getName() + " ORDER BY t1");

        ComparisonUtils.compareTables(hawqExternalTable, comparisonDataTable, report);
    }

    /**
     * use PXF Text connectors to get data from Hive partitioned table
     *
     * @throws Exception
     */
    @Test
    public void severalTextPartitions() throws Exception {

        HiveExternalTable hiveExternalTable = new HiveExternalTable("reg_heterogen", new String[] {
                "t0 string",
                "t1 string",
                "num1 int",
                "d1 double"});

        hiveExternalTable.setPartitionBy("fmt string");

        hive.createTableAndVerify(hiveExternalTable);

        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc1') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");
        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc2') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");
        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc3') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");

        // Create PXF Table for Hive portioned table
        ReadableExternalTable extTableNoProfile = TableFactory.getPxfHiveTextReadableTable("hv_heterogen", new String[] {
                "t1    text",
                "t2    text",
                "num1  integer",
                "dub1  double precision",
                "t3 text"}, hiveExternalTable, true);

        try {
            hawq.createTableAndVerify(extTableNoProfile);
        } catch (Exception e) {
            ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
        }

        hawq.queryResults(extTableNoProfile, "SELECT * FROM " + extTableNoProfile.getName() + " ORDER BY t3, t1");

        // pump up the small data to fit the unified data
        comparisonDataTable.loadDataFromFile(PxfHiveTextFixture.HIVE_SMALL_DATA_FILE_PATH, ",", 0);
        pumpUpComparisonTableData(3, false);

        ComparisonUtils.compareTables(extTableNoProfile, comparisonDataTable, report);
    }

    /**
     * Create HAWQ external table without the partition column. check error.
     *
     * @throws Exception
     */
    @Test
    public void severalTextPartitionsNoPartitonColumInHawq() throws Exception {

        HiveExternalTable hiveExternalTable = new HiveExternalTable("reg_heterogen", new String[] {
                "t0 string",
                "t1 string",
                "num1 int",
                "d1 double"});

        hiveExternalTable.setPartitionBy("fmt string");

        hive.createTableAndVerify(hiveExternalTable);

        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc1') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");
        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc2') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");
        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc3') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");

        /**
         * Create PXF Table using Hive profile
         */
        ReadableExternalTable extTableNoProfile = TableFactory.getPxfHiveTextReadableTable("hv_heterogen_using_profile", new String[] {
                "t1    text",
                "t2    text",
                "num1  integer",
                "dub1  double precision"}, hiveExternalTable, true);

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
                "d1 double"});

        hiveExternalTable.setPartitionBy("fmt string, part string");

        hive.createTableAndVerify(hiveExternalTable);

        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc1', part = 'a') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");
        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc2', part = 'b') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");
        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc3', part = 'c') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");

        /**
         * Create PXF Table using Hive profile
         */
        ReadableExternalTable extTableNoProfile = TableFactory.getPxfHiveTextReadableTable("hv_heterogen_using_filter", new String[] {
                "t1    text",
                "t2    text",
                "num1  integer",
                "dub1  double precision",
                "t3 text",
                "prt text"}, hiveExternalTable, false);

        try {
            hawq.createTableAndVerify(extTableNoProfile);
        } catch (Exception e) {
            ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
        }

        hawq.queryResults(extTableNoProfile, "SELECT * FROM " + extTableNoProfile.getName() + " WHERE t3 = 'rc1' AND prt = 'a' ORDER BY t3, t1");

        // pump up the small data to fit the unified data
        comparisonDataTable.loadDataFromFile(PxfHiveTextFixture.HIVE_SMALL_DATA_FILE_PATH, ",", 0);
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
                "d1 double"});

        hiveExternalTable.setPartitionBy("fmt string, part string");

        hive.createTableAndVerify(hiveExternalTable);

        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc1', part = 'a') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");
        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc2', part = 'b') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");
        hive.runQuery("ALTER TABLE " + hiveExternalTable.getName() + " ADD PARTITION (fmt = 'rc3', part = 'c') LOCATION 'hdfs:/hive/warehouse/" + hiveTextTable.getName() + "'");

        /**
         * Create PXF Table using Hive profile
         */
        ReadableExternalTable extTableNoProfile = TableFactory.getPxfHiveTextReadableTable("hv_heterogen_using_filter", new String[] {
                "t1    text",
                "t2    text",
                "num1  integer",
                "dub1  double precision",
                "t3 text",
                "prt text"}, hiveExternalTable, true);

        try {
            hawq.createTableAndVerify(extTableNoProfile);
        } catch (Exception e) {
            ExceptionUtils.validate(report, e, new Exception("nonstandard use of escape in a string literal"), true, true);
        }

        hawq.queryResults(extTableNoProfile, "SELECT * FROM " + extTableNoProfile.getName() + " WHERE num1 > 5 AND dub1 < 12 ORDER BY t3, t1");

        Table dataCompareTable = new Table("dataTable", null);

        // prepare expected data
        dataCompareTable.addRow(new String[] {"row6", "s_11", "6", "11", "rc1", "a"});
        dataCompareTable.addRow(new String[] {"row6", "s_11", "6", "11", "rc2", "b"});
        dataCompareTable.addRow(new String[] {"row6", "s_11", "6", "11", "rc3", "c"});

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
                "sr1 STRUCT<street:STRING,  city:STRING,  state:STRING,  zip:INT >"});

        hive.createTableAndVerify(hiveTable);

        hawqExternalTable = TableFactory.getPxfHiveTextReadableTable("hv_collections", new String[] {
                "t1    text",
                "f1    real",
                "t2    text",
                "t3    text",
                "t4    text"}, hiveTable, false);

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
        String[] arr = {"rc1", "rc2", "rc3"};
        String[] arr2 = {"a", "b", "c"};

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