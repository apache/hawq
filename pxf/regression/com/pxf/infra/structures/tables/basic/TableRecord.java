package com.pxf.infra.structures.tables.basic;

/**
 * Represents a record in a column for DB tables
 */
public class TableRecord {

	private String columnFamilyName;

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
	}
}
