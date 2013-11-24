package com.pxf.tests.basic;

import org.junit.Assert;
import org.junit.Test;

import com.pxf.infra.structures.tables.basic.Table;
import com.pxf.infra.structures.tables.pxf.ReadbleExternalTable;
import com.pxf.infra.structures.tables.pxf.WritableExternalTable;
import com.pxf.infra.structures.tables.utils.TableFactory;
import com.pxf.tests.testcases.PxfTestCase;

/**
 * PXF using Writable External Tables with different types of data
 */
public class PxfHdfsWritableRegression extends PxfTestCase {

	/**
	 * Create writable table, insert data to it, check that data is writen to
	 * HDFS, than read it with Readable Table.
	 * 
	 * @throws Exception
	 */
	@Test
	public void text() throws Exception {

		String hdfsDir = hdfsWorkingFolder + "/writable/wrtext";

		hdfs.getFunc().removeDirectory(hdfsDir);

		WritableExternalTable writableTable = TableFactory.getPxfWritableTextTable("wrtext", new String[] {
				"s1 text",
				"n1 int",
				"n2 int" }, hdfsDir, ",");

		createHawqTable(writableTable);

		Table sudoTable = new Table("sudoTable", null);

		sudoTable.addRow(new String[] { "first", "1", "11" });
		sudoTable.addRow(new String[] { "second", "2", "22" });

		hawq.insertData(sudoTable, writableTable);

		Assert.assertNotEquals(hdfs.getFunc().listSize(hdfsDir), 0);

		ReadbleExternalTable readbleTable = TableFactory.getPxfReadbleTextTable("retext", new String[] {
				"s1 text",
				"n1 int",
				"n2 int" }, hdfsDir, ",");

		createHawqTable(readbleTable);

		hawq.queryResults(readbleTable, "SELECT * FROM " + readbleTable.getName() + " ORDER BY s1");

		compareTables(readbleTable, sudoTable);
	}

	/**
	 * Create writable table with Gzip codec, insert data to it using copy, than
	 * read it from HDFS using Readable Table.
	 * 
	 * @throws Exception
	 */
	@Test
	public void gzip() throws Exception {

		String hdfsDir = hdfsWorkingFolder + "/writable/gzip";

		hdfs.getFunc().removeDirectory(hdfsDir);

		WritableExternalTable writableTable = TableFactory.getPxfWritableGzipTable("wrgzip", new String[] {
				"s1 text",
				"n1 int",
				"n2 int" }, hdfsDir, ",");

		createHawqTable(writableTable);

		Table sudoTable = new Table("sudoTable", null);

		for (int i = 0; i < 1000; i++) {
			sudoTable.addRow(new String[] {
					"gzip" + i,
					String.valueOf((i + 1)),
					String.valueOf((i * 2)) });
		}

		hawq.copy(sudoTable, writableTable);

		ReadbleExternalTable readbleTable = TableFactory.getPxfReadbleTextTable("readgzip", new String[] {
				"s1 text",
				"n1 int",
				"n2 int" }, hdfsDir, ",");

		createHawqTable(readbleTable);

		hawq.queryResults(readbleTable, "SELECT * FROM " + readbleTable.getName() + " ORDER BY n1");

		compareTables(readbleTable, sudoTable);
	}
}