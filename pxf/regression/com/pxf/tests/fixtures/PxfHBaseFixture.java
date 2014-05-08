package com.pxf.tests.fixtures;

import com.pivotal.parot.components.hbase.HBase;
import com.pivotal.parot.structures.tables.basic.Table;

public class PxfHBaseFixture extends BasicFixture {

	HBase hbase;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		startFixtureLevel();

		hbase = (HBase) system.getSystemObject("hbase");

		Table[] hbaseTables = new Table[] {
				new Table("hbase_table", null),
				new Table("hbase_table_with_nulls", null),
				new Table("hbase_table_integer_row_key", null),
				new Table("pxflookup", null) };

		for (int i = 0; i < hbaseTables.length; i++) {

			if (hbase.checkTableExists(hbaseTables[i])) {
				hbase.dropTable(hbaseTables[i], false);
			}
		}
		
		stopFixtureLevel();
	}
}
