package com.pxf.infra.utils.tables;

import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import com.pxf.infra.structures.tables.basic.Table;
import com.pxf.infra.utils.csv.CsvUtils;

/**
 * Utility for Comparing all sorts of types such Tables, CSV file
 */
public class ComparisonUtils {

	private static StringBuilder htmlReport;

	public static boolean compareCsvTableData(String pathToCsvFile, Table table)
			throws Exception {

		List<List<String>> tableData = table.getData();
		List<List<String>> csvAsList = CsvUtils.getList(pathToCsvFile);

		if (tableData.size() != csvAsList.size()) {
			throw new Exception("Table: " + table.getFullName() + " size = " + tableData.size() + " not equal to CSV file: " + pathToCsvFile + " size = " + csvAsList.size());
		}

		Table csvTable = new Table(pathToCsvFile, null);
		csvTable.setData(csvAsList);

		return compareTableData(table, csvTable);
	}

	public static boolean compareTableData(Table table1, Table table2)
			throws Exception {

		htmlReport = new StringBuilder();

		List<List<String>> t1Data = table1.getData();
		List<List<String>> t2Data = table2.getData();

		List<Integer> t1Types = table1.getColsDataType();
		List<Integer> t2Types = table2.getColsDataType();

		if (t1Data.size() != t2Data.size()) {
			throw new Exception("Table: " + table1.getFullName() + " size = " + t1Data.size() + " not equal to Table: " + table2.getFullName() + " size = " + t2Data.size());
		}

		boolean result = true;

		StringBuilder htmlFormat1 = new StringBuilder();
		htmlFormat1.append("<table border=\"1\">");
		htmlFormat1.append("<tr>");

		StringBuilder htmlFormat2 = new StringBuilder();
		htmlFormat2.append("<table border=\"1\">");

		if (t1Types != null) {

			htmlFormat2.append("<tr>");

			for (int i = 0; i < t1Types.size(); i++) {
				htmlFormat1.append("<td> " + t1Types.get(i) + "</td>");
			}

			htmlFormat1.append("</tr>");
		}

		if (t2Types != null) {

			htmlFormat2.append("<tr>");

			for (int i = 0; i < t2Types.size(); i++) {
				htmlFormat2.append("<td> " + t2Types.get(i) + "</td>");
			}

			htmlFormat2.append("</tr>");
		}

		for (int i = 0; i < t1Data.size(); i++) {

			result &= compareRowData(t1Types, t1Data.get(i), htmlFormat1, t2Types, t2Data.get(i), htmlFormat2);

			htmlFormat1.append("</tr>");
			htmlFormat2.append("</tr>");
		}

		htmlFormat1.append("</table>");
		htmlFormat2.append("</table>");

		htmlReport.append(table1.getFullName() + "<br>" + htmlFormat1 + "<br><br>" + table2.getFullName() + "<br>" + htmlFormat2);

		return result;
	}

	/**
	 * Check specific Column and return if equal
	 * 
	 * @param colType
	 * @param dataColT1
	 * @param dataColT2
	 * @return
	 * @throws ParseException
	 */
	private static boolean checkColData(int colType, String dataColT1, String dataColT2)
			throws ParseException {

		if (dataColT1.equals(dataColT2)) {
			return true;
		}

		if (colType == Types.DOUBLE) {
			if (Double.valueOf(dataColT1).doubleValue() == Double.valueOf(dataColT2)
					.doubleValue()) {

				return true;
			}
		}

		if (colType == Types.FLOAT || colType == Types.REAL) {
			if (Float.valueOf(dataColT1).floatValue() == Float.valueOf(dataColT2)
					.floatValue()) {

				return true;
			}
		}

		if (colType == Types.BOOLEAN || colType == Types.BIT) {

			String tempDataColT1 = (dataColT1.startsWith("t") ? "true" : "false");
			String tempDataColT2 = (dataColT2.startsWith("t") ? "true" : "false");

			if (Boolean.parseBoolean(tempDataColT1) == Boolean.parseBoolean(tempDataColT2)) {

				return true;
			}

		}

		if (colType == Types.TIMESTAMP) {

			String t1FormatedTS = dataColT1.replaceAll("-", "/");
			String t2FormatedTS = dataColT2.replaceAll("-", "/");

			if (t1FormatedTS.contains(".")) {
				t1FormatedTS = t1FormatedTS.substring(0, t1FormatedTS.indexOf("."));
			}

			if (t2FormatedTS.contains(".")) {
				t2FormatedTS = t2FormatedTS.substring(0, t2FormatedTS.indexOf("."));
			}

			DateFormat dateFormater = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

			if (dateFormater.parse(t1FormatedTS).getTime() == dateFormater.parse(t2FormatedTS)
					.getTime()) {

				return true;
			}
		}

		return false;
	}

	/**
	 * Compare Table rows
	 * 
	 * @param row1Types
	 *            types of row
	 * @param row1Data
	 *            data of row
	 * @param row1HtmlReport
	 *            html collector
	 * @param row2Types
	 *            types of row
	 * @param row2Data
	 *            data of row
	 * @param row2HtmlReport
	 *            html collector
	 * @return true if rows are equal
	 * @throws ParseException
	 */
	private static boolean compareRowData(List<Integer> row1Types, List<String> row1Data, StringBuilder row1HtmlReport, List<Integer> row2Types, List<String> row2Data, StringBuilder row2HtmlReport)
			throws ParseException {

		boolean result = true;
		
		for (int table1Index = 0, table2Index = 0; table1Index < row1Data.size(); table1Index++, table2Index++) {

			String dataColT1 = "null";
			String dataColT2 = "null";

			if (row1Data.get(table1Index) != null) {
				dataColT1 = row1Data.get(table1Index).trim();
			}

			if (row2Data.get(table2Index) != null) {
				dataColT2 = row2Data.get(table2Index).trim();
			}

			boolean isEqual = false;
			boolean arrayT1 = false;
			boolean arrayT2 = false;

			arrayT1 = false;
			arrayT2 = false;

			if (dataColT1.startsWith("[") && dataColT1.endsWith("]")) {

				arrayT2 = true;

				String[] array = dataColT1.substring(1, dataColT1.length() - 1)
						.split(",");

				for (int k = 0; k < array.length; k++) {

					isEqual = checkColData(row1Types.get(table1Index)
							.intValue(), array[k].trim(), row2Data.get(table2Index)
							.trim());
					result &= isEqual;
					
					row2HtmlReport.append("<td>" + ((isEqual) ? "" : "<font color=\"red\">") + row2Data.get(table2Index)
							.trim() + ((isEqual) ? "" : "</font>") + "</td>");

					if (k != array.length - 1) {
						table2Index++;
					}
				}
			} else

			if (dataColT2.startsWith("[") && dataColT2.endsWith("]")) {

				arrayT1 = true;

				String[] array = dataColT2.substring(1, dataColT2.length() - 1)
						.split(",");

				for (int k = 0; k < array.length; k++) {

					isEqual = checkColData(row1Types.get(table1Index)
							.intValue(), array[k].trim(), row1Data.get(table1Index)
							.trim());
					result &= isEqual;
					
					row1HtmlReport.append("<td>" + ((isEqual) ? "" : "<font color=\"red\">") + row1Data.get(table1Index)
							.trim() + ((isEqual) ? "" : "</font>") + "</td>");

					if (k != array.length - 1) {
						table1Index++;
					}
				}
			} else

			if (dataColT1.startsWith("{") && dataColT1.endsWith("}")) {

				arrayT2 = true;

				String[] array = dataColT1.substring(1, dataColT1.length() - 1)
						.split(",");

				for (int k = 0; k < array.length; k++, table2Index += 2) {

					String key = array[k].split("=")[0].trim();
					String value = array[k].split("=")[1].trim();

					isEqual = checkColData(row2Types.get(table2Index)
							.intValue(), key, row2Data.get(table2Index).trim());
					result &= isEqual;
					
					row2HtmlReport.append("<td>" + ((isEqual) ? "" : "<font color=\"red\">") + row2Data.get(table2Index)
							.trim() + ((isEqual) ? "" : "</font>") + "</td>");

					isEqual = checkColData(row2Types.get(table2Index + 1)
							.intValue(), value, row2Data.get(table2Index + 1)
							.trim());
					result &= isEqual;

					row2HtmlReport.append("<td>" + ((isEqual) ? "" : "<font color=\"red\">") + row2Data.get(table2Index + 1)
							.trim() + ((isEqual) ? "" : "</font>") + "</td>");

					if (k == array.length - 1) {
						table2Index--;
					}
				}

			} else

			if (dataColT2.startsWith("{") && dataColT2.endsWith("}")) {

				arrayT1 = true;

				String[] array = dataColT2.substring(1, dataColT2.length() - 1)
						.split(",");

				for (int k = 0; k < array.length; k++, table1Index += 2) {

					String key = array[k].split("=")[0].trim();
					String value = array[k].split("=")[1].trim();

					isEqual = checkColData(row1Types.get(table1Index)
							.intValue(), key, row1Data.get(table1Index).trim());
					result &= isEqual;
					
					row1HtmlReport.append("<td>" + ((isEqual) ? "" : "<font color=\"red\">") + row1Data.get(table1Index)
							.trim() + ((isEqual) ? "" : "</font>") + "</td>");

					isEqual = checkColData(row1Types.get(table1Index + 1)
							.intValue(), value, row1Data.get(table1Index + 1)
							.trim());
					result &= isEqual;

					row1HtmlReport.append("<td>" + ((isEqual) ? "" : "<font color=\"red\">") + row1Data.get(table1Index + 1)
							.trim() + ((isEqual) ? "" : "</font>") + "</td>");

					if (k == array.length - 1) {
						table1Index--;
					}
				}
			} else {

				isEqual = checkColData(row1Types.get(table1Index).intValue(), dataColT1, dataColT2);
				result &= isEqual;
			}

			if (!arrayT1) {
				row1HtmlReport.append("<td>" + ((isEqual) ? "" : "<font color=\"red\">") + dataColT1 + ((isEqual) ? "" : "</font>") + "</td>");
			}

			if (!arrayT2) {
				row2HtmlReport.append("<td>" + ((isEqual) ? "" : "<font color=\"red\">") + dataColT2 + ((isEqual) ? "" : "</font>") + "</td>");
			}
		}

		row1HtmlReport.append("</tr>");
		row2HtmlReport.append("</tr>");
		
		return result;
	}

	/***
	 * 
	 * @return HTML code with Comparison details
	 */
	public static String getHtmlReport() {
		return htmlReport.toString();
	}
}
