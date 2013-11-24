package com.pxf.infra.structures.tables.hbase;

import com.pxf.infra.structures.tables.basic.TableRecord;

/**
 * Represents HBase Table record contains all it components: rowKey, qualifier
 * and value.
 */
public class HBaseTableRecord extends TableRecord {

	private String rowKey;

	private String coulmnQualifier;

	private String value;

	public String getCoulmnQualifier() {
		return coulmnQualifier;
	}

	public void setCoulmnQualifier(String coulmnQualifier) {
		this.coulmnQualifier = coulmnQualifier;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {

		StringBuilder sb = new StringBuilder();

		sb.append("RowKey: " + rowKey + "\n");
		sb.append("Column Family: " + getColumnFamilyName() + "\n");
		sb.append("Column Qualifier: " + coulmnQualifier + "\n");
		sb.append("Value: " + value);

		return sb.toString();
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}
}
