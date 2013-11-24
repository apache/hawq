package com.pxf.tests.testcases;

import java.io.File;

import jsystem.framework.report.Reporter;
import jsystem.framework.report.Reporter.ReportAttribute;
import junit.framework.SystemTestCase4;

import org.junit.Assert;

import com.pxf.infra.hawq.Hawq;
import com.pxf.infra.hdfs.Hdfs;
import com.pxf.infra.structures.tables.basic.Table;
import com.pxf.infra.utils.jsystem.report.ReportUtils;
import com.pxf.infra.utils.tables.ComparisonUtils;

/**
 * All test cases that related to PXF will extends from this Test Case class.
 * Already includes HAWQ and HDFS system objects loaded and ready to use.
 */
public class PxfTestCase extends SystemTestCase4 {

	protected Hawq hawq;

	protected Hdfs hdfs;

	protected String hdfsWorkingFolder;

	@Override
	public void defaultBefore() throws Throwable {
		super.defaultBefore();

		hawq = (Hawq) system.getSystemObject("hawq");
		hdfs = (Hdfs) system.getSystemObject("hdfs");

		hdfsWorkingFolder = hdfs.getWorkingDirectory();
	}

	/**
	 * convenient method for creating HAWQ table - will drop the old table if
	 * exists create it and check if exists
	 * 
	 * @param table
	 * @throws Exception
	 */
	protected void createHawqTable(Table table) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Drop, Create and Check Table: " + table.getFullName());

		try {
			hawq.dropTable(table);
			hawq.createTable(table);

			Assert.assertEquals(true, hawq.checkTableExists(table.getSchema(), table.getName()));

		} catch (Exception e) {
			ReportUtils.stopLevel(report);

			throw e;
		}

		ReportUtils.stopLevel(report);
	}

	/**
	 * Compare t1 data with t2 data, will fail the test if false.
	 * 
	 * @param t1
	 * @param t2
	 * @throws Exception
	 */
	protected void compareTables(Table t1, Table t2) throws Exception {

		boolean comparationResult = ComparisonUtils.compareTableData(t1, t2);

		String reportTitle = "Tables Compartion " + ((comparationResult) ? " Passed" : "Failed");

		ReportUtils.reportHtmlLink(report, getClass(), reportTitle, ComparisonUtils.getHtmlReport(), ((comparationResult) ? Reporter.PASS : Reporter.FAIL));
	}

	/**
	 * compare CSV file data with Table data.
	 * 
	 * @param csvFile
	 * @param table
	 * @throws Exception
	 */
	protected void compareCsv(File csvFile, Table table) throws Exception {

		boolean comparationResult = ComparisonUtils.compareCsvTableData(csvFile.getAbsolutePath(), table);

		String reportTitle = "Csv Against Tables comparison " + ((comparationResult) ? " Passed" : "Failed");

		report.report(reportTitle, ComparisonUtils.getHtmlReport(), ((comparationResult) ? Reporter.PASS : Reporter.FAIL), ReportAttribute.HTML);
	}
}
