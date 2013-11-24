package com.pxf.infra.structures.tables.basic;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents DB table.
 */
public class Table {

	private StringBuilder dataHtml;

	private List<String> columnsHeaders;
	private List<List<String>> data;
	private List<Integer> colsDataType;
	private String schema;
	private String name;
	private String[] fields;
	private String[] distributionFields;

	public Table(String name, String[] fields) {
		this.name = name;
		this.fields = fields;
		init();
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String[] getFields() {
		return fields;
	}

	public void setFields(String[] fields) {
		this.fields = fields;
	}

	protected String createHeader() {
		return "CREATE TABLE " + getFullName();
	}

	protected String createFields() {

		StringBuilder sb = new StringBuilder();

		sb.append(" ( ");

		for (int i = 0; i < fields.length; i++) {
			sb.append(fields[i]);

			if (i != fields.length - 1) {
				sb.append(",  ");
			}
		}

		sb.append(" ) ");

		return sb.toString();
	}

	protected String distribution() {
		StringBuilder sb = new StringBuilder();

		if (getDistributionFields() != null && getDistributionFields().length > 0) {

			int arraySize = getDistributionFields().length;

			sb.append("DISTRIBUTED BY (");

			for (int i = 0; i < arraySize; i++) {
				sb.append(distributionFields[i]);

				if (i != (arraySize - 1)) {
					sb.append(", ");
				}
			}

			sb.append(")");
		}

		return sb.toString();
	}

	protected String createLocation() {
		return "";
	}

	/**
	 * Generates "Create Table" query
	 * 
	 * @return "create Table" query
	 */
	public String constructCreateStmt() {
		StringBuilder sb = new StringBuilder();

		sb.append(createHeader() + " ");
		sb.append(createFields() + " ");
		sb.append(distribution() + " ");

		return sb.toString();
	}

	/**
	 * Generates "Drop Table" query
	 * 
	 * @return "Drop Table" query
	 */
	public String constructDropStmt() {

		StringBuilder sb = new StringBuilder();

		sb.append("DROP TABLE IF EXISTS " + getFullName());

		return sb.toString();
	}

	/**
	 * returns table gull name if exists: "schema.table"
	 * 
	 * @return
	 */
	public String getFullName() {

		if (schema != null) {
			return (schema + "." + name);
		}
		return name;
	}

	/**
	 * Add Row to Data List in List format.
	 * 
	 * @param row
	 */
	public void addRow(List<String> row) {
		if (data == null) {
			data = new ArrayList<List<String>>();
		}

		data.add(row);

	}

	/**
	 * Add Row to Data List using String[]
	 * 
	 * @param row
	 */
	public void addRow(String[] row) {

		ArrayList<String> rowList = new ArrayList<String>();

		for (int i = 0; i < row.length; i++) {
			rowList.add(row[i]);
		}

		addRow(rowList);

	}

	/**
	 * Generates HTML table from Data List
	 * 
	 * @return
	 */
	public String getDataHtml() {

		dataHtml = new StringBuilder();

		dataHtml.append("<table border=\"1\">");

		/**
		 * Add headers
		 */
		dataHtml.append("<tr bgcolor=\"#c1cdc1\">");

		for (int i = 0; i < columnsHeaders.size(); i++) {
			dataHtml.append("<td> " + columnsHeaders.get(i) + "</td>");
		}

		dataHtml.append("</tr>");

		dataHtml.append("<tr>");

		for (int i = 0; i < data.size(); i++) {

			List<String> row = data.get(i);

			for (int j = 0; j < row.size(); j++) {
				dataHtml.append("<td> " + row.get(j) + "</td>");
			}

			dataHtml.append("</tr>");

		}

		dataHtml.append("</table>");

		return dataHtml.toString();
	}

	/**
	 * Add data type to colsDataType list
	 * 
	 * @param dataType
	 */
	public void addColDataType(int dataType) {
		if (colsDataType == null) {
			colsDataType = new ArrayList<Integer>();
		}

		colsDataType.add(dataType);
	}

	public void addColumnHeader(String header) {
		if (columnsHeaders == null) {
			columnsHeaders = new ArrayList<String>();
		}

		columnsHeaders.add(header);
	}

	/**
	 * resets the data and colsDataType lists.
	 */
	public void init() {
		colsDataType = new ArrayList<Integer>();
		data = new ArrayList<List<String>>();
		columnsHeaders = new ArrayList<String>();
	}

	public List<Integer> getColsDataType() {
		return colsDataType;
	}

	public List<List<String>> getData() {
		return data;
	}

	public void setData(List<List<String>> data) {
		this.data = data;
	}

	public String[] getDistributionFields() {
		return distributionFields;
	}

	public void setDistributionFields(String[] distributionFields) {
		this.distributionFields = distributionFields;
	}

}