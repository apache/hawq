package com.pivotal.pxf.utilities;

/*
 * ColumnDescriptor describes one column in hawq database.
 * Currently it means a name, a type id (HAWQ/GPDB OID), and a type name.
 */
public class ColumnDescriptor {
	int gpdbColumnTypeCode;
	String gpdbColumnName;
	String gpdbColumnTypeName;
	int gpdbColumnIndex;
	public static final String RECORD_KEY_NAME = "recordkey";

	public ColumnDescriptor(String name, int typecode, int index, String typename) {
		gpdbColumnTypeCode = typecode;
		gpdbColumnTypeName = typename;
		gpdbColumnName = name;
		gpdbColumnIndex = index;
	}

	public ColumnDescriptor(ColumnDescriptor copy) {
		this.gpdbColumnTypeCode = copy.gpdbColumnTypeCode;
		this.gpdbColumnName = copy.gpdbColumnName;
		this.gpdbColumnIndex = copy.gpdbColumnIndex;
		this.gpdbColumnTypeName = copy.gpdbColumnTypeName;
	}

	public String columnName() {
		return gpdbColumnName;
	}

	public int columnTypeCode() {
		return gpdbColumnTypeCode;
	}

	public int columnIndex() {
		return gpdbColumnIndex;
	}

	public String columnTypeName() {
		return gpdbColumnTypeName;
	}

	public boolean isKeyColumn() {
		return gpdbColumnName.compareToIgnoreCase(RECORD_KEY_NAME) == 0;
	}
}
