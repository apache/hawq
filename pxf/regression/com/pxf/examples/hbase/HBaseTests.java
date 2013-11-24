package com.pxf.examples.hbase;

import junit.framework.SystemTestCase4;

import org.junit.Before;
import org.junit.Test;

import com.pxf.infra.hbase.HBase;
import com.pxf.infra.hdfs.Hdfs;
import com.pxf.infra.structures.tables.hbase.HBaseTable;
import com.pxf.infra.structures.tables.hbase.HBaseTableRecord;

public class HBaseTests extends SystemTestCase4 {

	HBase hbase;
	Hdfs hdfs;

	@Before
	public void setUp() throws Exception {

		hbase = (HBase) system.getSystemObject("hbase");

		hdfs = (Hdfs) system.getSystemObject("hdfs");
	}

	@Test
	public void createTableTest() throws Exception {

		HBaseTable hbaseTable = new HBaseTable("Employee", new String[] { "name" });

		hbase.dropTable(hbaseTable);

		hbase.getTableList(null);

		hbase.createTable(hbaseTable);

		hbase.getTableList(null);

		for (int i = 0; i < 50; i++) {

			HBaseTableRecord record = new HBaseTableRecord();

			record.setColumnFamilyName("name");
			record.setRowKey(String.valueOf(i + 1));
			record.setCoulmnQualifier("last_name");
			record.setValue("yes_" + (i + 1));

			hbase.putRecord(hbaseTable, record);
		}

		hbase.getTableData(hbaseTable);

		HBaseTableRecord record = new HBaseTableRecord();

		record.setRowKey("30");

		hbase.getRecord(hbaseTable, record);

	}

	@Test
	public void loadBulkTest() throws Exception {

		// org.apache.hadoop.hbase.mapreduce.ImportTsv
		// -Dimporttsv.columns=HBASE_ROW_KEY,basic_info:empname,basic_info:age
		// employee /myTestDir/myimport22

		HBaseTable hbaseTable = new HBaseTable("employee", new String[] { "basic_info" });

		hdfs.getFunc().removeDirectory("importtsv_inputs");
		hdfs.getFunc().createDirectory("importtsv_inputs");
		hdfs.getFunc()
				.copyFromLocal("regression/resources/myimport", "importtsv_inputs/myimport");

		hbase.dropTable(hbaseTable);

		hbase.createTable(hbaseTable);

		hbase.getTableData(hbaseTable);

		hbase.loadBulk(hbaseTable, "importtsv_inputs/myimport", new String[] {
				"basic_info:empname",
				"basic_info:age" });

		hbase.getTableData(hbaseTable);
	}
}