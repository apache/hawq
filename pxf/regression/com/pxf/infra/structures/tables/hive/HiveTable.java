package com.pxf.infra.structures.tables.hive;

import com.pxf.infra.structures.tables.basic.Table;

/**
 * Represents Hive Table
 */
public class HiveTable extends Table {

	private String format;

	private String delimiterFieldsBy;

	private String delimiterCollectionItemsBy;

	private String delimiterMapKeysBy;

	private String delimiterLinesBy;

	private String storedAs;

	private String partitionBy;

	public HiveTable(String name, String[] fields) {
		super(name, fields);
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getDelimiterFieldsBy() {
		return delimiterFieldsBy;
	}

	public void setDelimiterFieldsBy(String delimiterFieldsBy) {
		this.delimiterFieldsBy = delimiterFieldsBy;
	}

	@Override
	public String constructCreateStmt() {

		StringBuilder sb = new StringBuilder();

		sb.append(super.constructCreateStmt());

		if (partitionBy != null) {
			sb.append(" partitioned by (" + partitionBy + ") ");
		}

		if (format != null) {
			sb.append(" " + format + " format ");

		}

		if (delimiterFieldsBy != null) {

			sb.append("delimited fields terminated by '" + delimiterFieldsBy + "' ");
		}

		if (delimiterCollectionItemsBy != null) {

			sb.append("collection items terminated by '" + delimiterCollectionItemsBy + "' ");
		}

		if (delimiterMapKeysBy != null) {

			sb.append("map keys terminated by '" + delimiterMapKeysBy + "' ");
		}

		if (delimiterLinesBy != null) {

			sb.append("lines terminated by '" + delimiterLinesBy + "' ");
		}

		if (storedAs != null) {
			sb.append(" STORED AS " + storedAs);
		}

		return sb.toString();
	}

	public String getStoredAs() {
		return storedAs;
	}

	public void setStoredAs(String storedAs) {
		this.storedAs = storedAs;
	}

	public String getPartitionBy() {
		return partitionBy;
	}

	public void setPartitionBy(String partitionBy) {
		this.partitionBy = partitionBy;
	}

	public String getDelimiterCollectionItemsBy() {
		return delimiterCollectionItemsBy;
	}

	public void setDelimiterCollectionItemsBy(String delimiterCollectionItemsBy) {
		this.delimiterCollectionItemsBy = delimiterCollectionItemsBy;
	}

	public String getDelimiterMapKeysBy() {
		return delimiterMapKeysBy;
	}

	public void setDelimiterMapKeysBy(String delimiterMapKeysBy) {
		this.delimiterMapKeysBy = delimiterMapKeysBy;
	}

	public String getDelimiterLinesBy() {
		return delimiterLinesBy;
	}

	public void setDelimiterLinesBy(String delimiterLinesBy) {
		this.delimiterLinesBy = delimiterLinesBy;
	}
}