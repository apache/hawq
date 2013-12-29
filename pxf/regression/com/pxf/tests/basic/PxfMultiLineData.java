package com.pxf.tests.basic;

import org.junit.Test;

import com.pivotal.pxfauto.infra.structures.tables.basic.Table;
import com.pivotal.pxfauto.infra.structures.tables.pxf.ReadableExternalTable;
import com.pivotal.pxfauto.infra.utils.fileformats.FileFormatsUtils;
import com.pivotal.pxfauto.infra.utils.tables.ComparisonUtils;
import com.pxf.tests.dataprepares.text.MultiLinePreparer;
import com.pxf.tests.testcases.PxfTestCase;

public class PxfMultiLineData extends PxfTestCase {
	/**
	 * Use "HdfsDataFragmenter" + "LineReaderAccessor" + "StringPassResolver"
	 * and TEXT delimiter for parsing CSV file.
	 * 
	 * @throws Exception
	 */
	@Test
	public void textFormatsManyFields() throws Exception {

		String textFilePath = hdfsWorkingFolder + "/multiblock.csv";
		String localDataFile = loaclTempFolder + "/multiblock.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new MultiLinePreparer(), 1000,
				dataTable);
		FileFormatsUtils.prepareDataFile(dataTable, 32000, localDataFile);

		hdfs.copyFromLocal(localDataFile, textFilePath);

		ReadableExternalTable exTable = new ReadableExternalTable("mbt", new String[] {
				"t1 text", "a1 integer" }, textFilePath, "TEXT");

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("LineBreakAccessor");
		exTable.setResolver("StringPassResolver");
		exTable.setDelimiter(",");

		hawq.createTableAndVerify(exTable);

		int limit = 10;

		hawq.queryResults(exTable, "SELECT t1, a1 FROM " + exTable.getName()
				+ " ORDER BY t1 LIMIT " + limit);

		dataTable.initDataStructures();

		for (int i = 0; i < limit; i++) {
			dataTable.addRow(new String[] { "t1", "1" });
		}

		ComparisonUtils.compareTables(exTable, dataTable, report, 10, new
				String[] {});

		hawq.runAnalyticQuery("SELECT COUNT(*) from " + exTable.getName(), "32000000");

		hawq.runAnalyticQuery("SELECT SUM(a1) from " + exTable.getName(), "16016000000");

		hawq.runAnalyticQuery("SELECT cnt < 32768000 AS check FROM (SELECT COUNT(*) AS cnt FROM " + exTable.getName() + " WHERE gp_segment_id = 0) AS a", "t");
	}
}
