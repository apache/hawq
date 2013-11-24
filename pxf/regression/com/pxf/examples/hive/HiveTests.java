package com.pxf.examples.hive;

import java.io.File;

import junit.framework.SystemTestCase4;

import org.junit.Test;

import com.pxf.infra.cluster.SingleCluster;
import com.pxf.infra.hive.Hive;
import com.pxf.infra.structures.tables.basic.Table;
import com.pxf.infra.structures.tables.hive.HiveTable;

public class HiveTests extends SystemTestCase4 {

	@Test
	public void createTableAndLoadData() throws Exception {

		SingleCluster sc = (SingleCluster) system.getSystemObject("sc");

		sc.startHiveServer();

		Hive hive = (Hive) system.getSystemObject("hive");

		HiveTable hiveTable = new HiveTable("testHiveDriverTable", new String[] {
				"key int",
				"value string" });

		hive.dropTable(hiveTable);

		hive.createTable(hiveTable);

		hive.loadData(hiveTable, new File("regression/resources/hive_data").getAbsolutePath());

		/**
		 * Will not use map reduce
		 */
		hive.queryResults(hiveTable, "select * from testHiveDriverTable");

		/**
		 * Will use map reduce
		 */
		hive.queryResults(hiveTable, "select * from testHiveDriverTable where key == 1");
	}

	@Test
	public void dropTable() throws Exception {

		SingleCluster sc = (SingleCluster) system.getSystemObject("cluster");

		sc.startHiveServer();

		Hive hive = (Hive) system.getSystemObject("hive");

		hive.dropTable(new Table("bla", null));
	}

}
