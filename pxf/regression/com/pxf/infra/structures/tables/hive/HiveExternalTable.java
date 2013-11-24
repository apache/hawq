package com.pxf.infra.structures.tables.hive;

/**
 * represents Hive External Table.
 */
public class HiveExternalTable extends HiveTable {

	public HiveExternalTable(String name, String[] fields) {
		super(name, fields);
	}

	@Override
	protected String createHeader() {
		return "CREATE EXTERNAL TABLE " + getFullName() + " ";
	}

}
