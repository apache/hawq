package com.pxf.infra.structures.tables.utils;

import com.pxf.infra.structures.tables.hive.HiveExternalTable;
import com.pxf.infra.structures.tables.hive.HiveTable;
import com.pxf.infra.structures.tables.pxf.ReadbleExternalTable;
import com.pxf.infra.structures.tables.pxf.WritableExternalTable;

/**
 * Factory for creating different kind of Tables configuration more easily.
 */
public abstract class TableFactory {

	/**
	 * 
	 * @param tableName
	 * @param fields
	 * @param path
	 * @return PXF Readable External Table using "Hive" profile
	 */
	public static ReadbleExternalTable getPxfHiveReadbleTable(String tableName, String[] fields, String path) {

		ReadbleExternalTable exTable = new ReadbleExternalTable(tableName, fields, path, "custom");

		exTable.setProfile("Hive");
		exTable.setFormatter("pxfwritable_import");

		return exTable;
	}

	/**
	 * 
	 * @param name
	 * @param fields
	 * @param path
	 * @param delimiter
	 * @return PXF Readable External Table using "HdfsTextSimple" profile and
	 *         "Text" format.
	 */
	public static ReadbleExternalTable getPxfReadbleTextTable(String name, String[] fields, String path, String delimiter) {
		ReadbleExternalTable exTable = new ReadbleExternalTable(name, fields, path, "Text");

		exTable.setProfile("HdfsTextSimple");
		exTable.setDelimiter(delimiter);

		return exTable;
	}

	/**
	 * 
	 * @param name
	 * @param fields
	 * @param path
	 * @param delimiter
	 * @return PXF Writable External Table using "HdfsTextSimple" profile and
	 *         "Text" format.
	 */
	public static WritableExternalTable getPxfWritableTextTable(String name, String[] fields, String path, String delimiter) {

		WritableExternalTable exTable = new WritableExternalTable(name, fields, path, "Text");

		exTable.setProfile("HdfsTextSimple");
		exTable.setDelimiter(delimiter);

		return exTable;
	}

	/**
	 * 
	 * @param name
	 * @param fields
	 * @param path
	 * @return PXF Writable External Table using "HdfsTextSimple" profile and
	 *         "GzipCodec" codec compression.
	 */
	public static WritableExternalTable getPxfWritableGzipTable(String name, String[] fields, String path, String delimiter) {

		WritableExternalTable exTable = new WritableExternalTable(name, fields, path, "Text");

		exTable.setProfile("HdfsTextSimple");
		exTable.setDelimiter(delimiter);
		exTable.setCompressionCodec("org.apache.hadoop.io.compress.GzipCodec");

		return exTable;
	}

	/**
	 * 
	 * @param name
	 * @param fields
	 * @return Hive Table with "row" format and comma Delimiter.
	 */
	public static HiveTable getHivebyRowCommaTable(String name, String[] fields) {

		HiveTable table = new HiveTable(name, fields);

		table.setFormat("row");
		table.setDelimiterFieldsBy(",");

		return table;
	}

	/**
	 * 
	 * @param name
	 * @param fields
	 * @return Hive External Table with "row" format and comma Delimiter.
	 */
	public static HiveExternalTable getHiveByRowCommaExternalTable(String name, String[] fields) {

		HiveExternalTable table = new HiveExternalTable(name, fields);

		table.setFormat("row");
		table.setDelimiterFieldsBy(",");

		return table;
	}
}
