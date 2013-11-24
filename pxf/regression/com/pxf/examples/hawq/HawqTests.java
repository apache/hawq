package com.pxf.examples.hawq;

import java.io.File;
import java.util.ArrayList;

import junit.framework.SystemTestCase4;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.pxf.infra.hawq.Hawq;
import com.pxf.infra.structures.tables.basic.Table;

public class HawqTests extends SystemTestCase4 {

	Hawq hawq;

	String workingSchema = "learn_schema";

	@Before
	public void setUp() throws Exception {
		hawq = (Hawq) system.getSystemObject("hawq");
	}

	@Test
	public void schema() throws Exception {
		System.out.println(hawq.getDataBasesList());
	}

	@Test
	public void createSchemaTablesAndAlter() throws Exception {

		hawq.runQuery("CREATE TABLE " + workingSchema + ".my_table_ro (ts timestamp, numa integer, name text) with (appendonly=true, orientation=row)");

		hawq.runQuery("CREATE TABLE " + workingSchema + ".my_table_co (like learn_schema.my_table_ro) with (appendonly=true, orientation=column)");

		hawq.getTableList(workingSchema);

		hawq.runQuery("ALTER TABLE learn_schema.my_table_ro ALTER COLUMN numa TYPE VARCHAR(10)");
		hawq.runQuery("ALTER TABLE learn_schema.my_table_co ALTER COLUMN numa TYPE VARCHAR(10)");
	}

	@Test
	public void insertData() throws Exception {

		hawq.runQuery("CREATE TABLE " + workingSchema + ".my_table_ro (ts timestamp, numa integer, name text) with (appendonly=true, orientation=row)");

		hawq.runQuery("CREATE TABLE " + workingSchema + ".my_table_co (like learn_schema.my_table_ro) with (appendonly=true, orientation=column)");

		hawq.getTableList(workingSchema);

		hawq.runQuery("INSERT INTO " + workingSchema + ".my_table_ro VALUES ('02-11-1977', 12, 'cheese'), ('01-12-1980', 23, 'bluband'), ('12-10-2011', 34, 'freeze'),('02-11-2013', 12, 'flacs')");

		Table table = new Table("my_table_ro", null);

		hawq.queryResults(table, "select  * from learn_schema.my_table_ro");

	}

	@Test
	public void copyFromCsv() throws Exception {

		hawq.runQuery("CREATE TABLE " + workingSchema + ".my_table_ro (ts timestamp, numa integer, name text) with (appendonly=true, orientation=row)");

		hawq.runQuery("CREATE TABLE " + workingSchema + ".my_table_co (like learn_schema.my_table_ro) with (appendonly=true, orientation=column)");

		hawq.getTableList(workingSchema);

		File resourceCsvFile = new File("resources/myData.csv");

		hawq.runQuery("COPY " + workingSchema + ".my_table_ro FROM '" + resourceCsvFile.getAbsolutePath() + "' CSV");

		Table table = new Table("my_table_ro", null);
		hawq.queryResults(table, "SELECT * from " + workingSchema + ".my_table_ro");

		Assert.assertEquals(1000, table.getData().size());
	}

	@Test
	public void queryTables() throws Exception {

		String tableEmployee = "employee";
		String tableOccupation = "occupation";

		hawq.runQuery("CREATE TABLE " + workingSchema + "." + tableEmployee + " (id integer, name text, occupationId integer) with (appendonly=true, orientation=row)");

		hawq.runQuery("CREATE TABLE " + workingSchema + "." + tableOccupation + " (id integer, name text) with (appendonly=true, orientation=row)");

		hawq.getTableList(workingSchema);

		hawq.runQuery("INSERT INTO " + workingSchema + "." + tableEmployee + " VALUES (1, 'yacov', 1), (2, 'boris', 1), (3, 'leon', 1),(4, 'moshe', 2)");

		hawq.runQuery("INSERT INTO " + workingSchema + "." + tableOccupation + " VALUES (1, 'engineer'), (2, 'manager')");

		hawq.queryResults(new Table(tableOccupation, null), "select  * from " + workingSchema + "." + tableOccupation);
		hawq.queryResults(new Table(tableEmployee, null), "select  * from " + workingSchema + "." + tableEmployee);

		hawq.queryResults(new Table("employee", null), "select a.name, b.name from learn_schema.employee a inner join learn_schema.occupation b on a.occupationId = b.id order by a.id");

	}

	@Test
	public void createDistributedTableByDay() throws Exception {

		hawq.runQuery("CREATE TABLE " + workingSchema + ".sales (id int, date date, amt decimal(10,2)) " + "DISTRIBUTED BY (id) " + "PARTITION BY RANGE (date) " + "(START (date '2008-01-01') INCLUSIVE END (date '2009-01-01') EXCLUSIVE EVERY (INTERVAL '1 day') )");

		ArrayList<String> tablesList = hawq.getTableList(workingSchema);

		Assert.assertEquals(367, tablesList.size());
	}

	@Test
	public void createDistributedTableByMonth() throws Exception {

		hawq.runQuery("CREATE TABLE " + workingSchema + ".sales (id int, date date, amt decimal(10,2)) " + "DISTRIBUTED BY (id) " + "PARTITION BY RANGE (date) " + "(START (date '2008-01-01') INCLUSIVE END (date '2009-01-01') EXCLUSIVE EVERY (INTERVAL '1 month') )");

		ArrayList<String> tablesList = hawq.getTableList(workingSchema);

		Assert.assertEquals(13, tablesList.size());
	}

	@Test
	public void createDistributedTableByYear() throws Exception {

		hawq.runQuery("CREATE TABLE " + workingSchema + ".sales (id int, date date, amt decimal(10,2)) " + "DISTRIBUTED BY (id) " + "PARTITION BY RANGE (date) " + "(START (date '2000-01-01') INCLUSIVE END (date '2010-01-01') EXCLUSIVE EVERY (INTERVAL '1 year') )");

		ArrayList<String> tablesList = hawq.getTableList(workingSchema);

		Assert.assertEquals(11, tablesList.size());
	}

	@Test
	public void createDistributionTableByNames() throws Exception {
		hawq.runQuery("CREATE TABLE " + workingSchema + ".sales (id int, date date, amt decimal(10,2)) DISTRIBUTED BY (id) " + "PARTITION BY RANGE (date) " + "( PARTITION Jan08 START (date '2008-01-01') INCLUSIVE , " + "PARTITION Feb08 START (date '2008-02-01') INCLUSIVE , " + "PARTITION Mar08 START (date '2008-03-01') INCLUSIVE , " + "PARTITION Apr08 START (date '2008-04-01') INCLUSIVE , " + "PARTITION May08 START (date '2008-05-01') INCLUSIVE , " + "PARTITION Jun08 START (date '2008-06-01') INCLUSIVE , " + "PARTITION Jul08 START (date '2008-07-01') INCLUSIVE , " + "PARTITION Aug08 START (date '2008-08-01') INCLUSIVE , " + "PARTITION Sep08 START (date '2008-09-01') INCLUSIVE , " + "PARTITION Oct08 START (date '2008-10-01') INCLUSIVE , " + "PARTITION Nov08 START (date '2008-11-01') INCLUSIVE , " + "PARTITION Dec08 START (date '2008-12-01') INCLUSIVE " + "END (date '2009-01-01') EXCLUSIVE )");

		ArrayList<String> tablesList = hawq.getTableList(workingSchema);

		Assert.assertEquals(13, tablesList.size());

	}

	@Test
	public void distributionNumericRange() throws Exception {

		hawq.runQuery("CREATE TABLE " + workingSchema + ".rank (id int, rank int, year int, gender " + "char(1), count int) " + "DISTRIBUTED BY (id) " + "PARTITION BY RANGE (year) " + "( START (2001) END (2008) EVERY (1), " + "DEFAULT PARTITION extra )");

		ArrayList<String> tablesList = hawq.getTableList(workingSchema);

		int master = 1, yearPart = 8;

		Assert.assertEquals(master + yearPart, tablesList.size());
	}

	@Test
	public void distributionList() throws Exception {

		hawq.runQuery("CREATE TABLE " + workingSchema + ".rank (id int, rank int, year int, gender " + "char(1), count int ) " + "DISTRIBUTED BY (id) " + "PARTITION BY LIST (gender) " + "( PARTITION girls VALUES ('F'), " + "PARTITION boys VALUES ('M'), " + "DEFAULT PARTITION other )");

		ArrayList<String> tablesList = hawq.getTableList(workingSchema);

		int master = 1, genderPart = 3;

		Assert.assertEquals(master + genderPart, tablesList.size());
	}

	@Test
	public void distribution2Layers() throws Exception {

		hawq.runQuery("CREATE TABLE " + workingSchema + ".sales (trans_id int, date date, amount " + "decimal(9,2), region text) " + "DISTRIBUTED BY (trans_id) " + "PARTITION BY RANGE (date) " + "SUBPARTITION BY LIST (region) " + "SUBPARTITION TEMPLATE " + "( SUBPARTITION usa VALUES ('usa'), " + "SUBPARTITION asia VALUES ('asia'), " + "SUBPARTITION europe VALUES ('europe'), " + "DEFAULT SUBPARTITION other_regions) " + "(START (date '2008-01-01') INCLUSIVE " + "END (date '2009-01-01') EXCLUSIVE " + "EVERY (INTERVAL '1 month'), " + "DEFAULT PARTITION outlying_dates )");

		ArrayList<String> tablesList = hawq.getTableList(workingSchema);

		int masterTable = 1, datePart = 13, regionPart = 4;

		Assert.assertEquals((masterTable + datePart + (regionPart * datePart)), tablesList.size());

		// hawq.runQuery("ALTER TABLE "
		// + workingSchema
		// +
		// ".sales SET SUBPARTITION TEMPLATE (SUBPARTITION usa VALUES ('usa'), SUBPARTITION asia VALUES ('asia'),  SUBPARTITION europe VALUES ('europe'), SUBPARTITION africa VALUES ('africa'), DEFAULT SUBPARTITION other )");
		//
		// regionPart = 5;
		//
		// Assert.assertEquals((masterTable + datePart + (regionPart *
		// datePart)),
		// tablesList.size());

	}

	@Test
	public void distribution3Layers() throws Exception {

		hawq.runQuery(" CREATE TABLE " + workingSchema + ".sales (id int, year int, month int, day int, region text) " + "DISTRIBUTED BY (id) PARTITION BY RANGE (year) " + "SUBPARTITION BY RANGE (month) SUBPARTITION TEMPLATE ( " + "START (1) END (13) EVERY (1), " + "DEFAULT SUBPARTITION other_months ) " + "SUBPARTITION BY LIST (region) " + "SUBPARTITION TEMPLATE ( " + "SUBPARTITION usa VALUES ('usa'), " + "SUBPARTITION europe VALUES ('europe'), " + "SUBPARTITION asia VALUES ('asia'), " + "DEFAULT SUBPARTITION other_regions ) " + "( START (2002) END (2010) EVERY (1), " + "DEFAULT PARTITION outlying_years )");

		ArrayList<String> tablesList = hawq.getTableList(workingSchema);

		int masterTable = 1, yearPart = 9, monthSubPart = 13, regionSubPart = 4;

		Assert.assertEquals((masterTable + (yearPart * monthSubPart * regionSubPart) + (yearPart * monthSubPart) + yearPart), tablesList.size());
	}

	@Test
	public void externalTextFiles() throws Exception {

		String exTableName = "ext_customer";

		File localFolder = new File("resources");

		hawq.runQuery("CREATE EXTERNAL TABLE learn_schema." + exTableName + "(id int, name text) LOCATION ( 'file://isxxvosgl1c.corp.emc.com" + localFolder.getAbsolutePath() + "/*.txt' ) FORMAT 'TEXT' ( DELIMITER '|') SEGMENT REJECT LIMIT 10;");

		hawq.queryResults(new Table(exTableName, null), "select * from " + workingSchema + "." + exTableName);

		hawq.queryResults(new Table(exTableName, null), "select id from " + workingSchema + "." + exTableName + " order by id");

		String targetTable = "targetTable";

		hawq.runQuery("CREATE TABLE " + workingSchema + "." + targetTable + " (LIKE " + workingSchema + "." + exTableName + ")");

		hawq.runQuery("INSERT INTO " + workingSchema + "." + targetTable + " select * from " + workingSchema + "." + exTableName);

		hawq.queryResults(new Table(targetTable, null), "select * from " + workingSchema + "." + targetTable);

		hawq.runQuery("CREATE WRITABLE EXTERNAL TABLE learn_schema.ext_wr_customer (LIKE " + workingSchema + "." + targetTable + ") LOCATION('gpfdist://localhost:8085/result.out') FORMAT 'TEXT' ( DELIMITER '|')");

		hawq.runQuery("insert into " + workingSchema + ".ext_wr_customer select * from " + workingSchema + "." + targetTable);
	}

	@Test
	public void externalCsvFiles() throws Exception {

		String exTableName = "ext_customer";

		File localFolder = new File("resources");

		hawq.runQuery("CREATE EXTERNAL TABLE learn_schema." + exTableName + "(ts timestamp, id int,name text) LOCATION ( 'file://isxxvosgl1c.corp.emc.com" + localFolder.getAbsolutePath() + "/*.csv' ) FORMAT 'CSV' ( DELIMITER ',') SEGMENT REJECT LIMIT 10;");

		hawq.queryResults(new Table(exTableName, null), "select * from " + workingSchema + "." + exTableName);

		hawq.queryResults(new Table(exTableName, null), "select id from " + workingSchema + "." + exTableName + " order by id");

		String targetTable = "targetTable";

		hawq.runQuery("CREATE TABLE " + workingSchema + "." + targetTable + " (LIKE " + workingSchema + "." + exTableName + ")");

		hawq.runQuery("INSERT INTO " + workingSchema + "." + targetTable + " select * from " + workingSchema + "." + exTableName);

		hawq.queryResults(new Table(targetTable, null), "select * from " + workingSchema + "." + targetTable);

		hawq.runQuery("CREATE WRITABLE EXTERNAL TABLE learn_schema.ext_wr_customer (LIKE " + workingSchema + "." + targetTable + ") LOCATION('gpfdist://localhost:8085/result2.out') FORMAT 'TEXT' ( DELIMITER '|')");

		hawq.runQuery("insert into " + workingSchema + ".ext_wr_customer select * from " + workingSchema + "." + targetTable);
	}

	@Test
	public void dropIfExists() throws Exception {
		hawq.dropTable(new Table("bla", null));
	}
}
