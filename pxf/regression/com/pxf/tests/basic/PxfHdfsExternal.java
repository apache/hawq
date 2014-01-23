package com.pxf.tests.basic;

import org.junit.Test;
import org.junit.After;

import org.postgresql.util.PSQLException;

import com.pivotal.pxfauto.infra.structures.tables.basic.Table;
import com.pivotal.pxfauto.infra.structures.tables.pxf.ReadableExternalTable;
import com.pivotal.pxfauto.infra.structures.tables.pxf.WritableExternalTable;
import com.pivotal.pxfauto.infra.utils.exception.ExceptionUtils;
import com.pivotal.pxfauto.infra.utils.tables.ComparisonUtils;
import com.pxf.tests.testcases.PxfTestCase;

/**
 * PXF on HDFS external plugins Regression tests
 */
public class PxfHdfsExternal extends PxfTestCase {

	/**
	 * Create PXF external readable table using external plugins for Fragmenter,
	 * Accessor , Resolver and Analyzer.
	 * 
	 * @throws Exception
	 */
	@Test
	public void readbleTableExteranlPlugins() throws Exception {

		/**
		 * gphdfs_in
		 */
		ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
				"num1 integer",
				"t1 text",
				"num2 integer" }, "regression_location", "CUSTOM");

		exTable.setFragmenter("DummyFragmenter");
		exTable.setAccessor("DummyAccessor");
		exTable.setResolver("DummyResolver");
		exTable.setAnalyzer("DummyAnalyzer");
		exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);
		hawq.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");

		Table dataCompareTable = new Table("dataCompareTable", null);

		dataCompareTable.addRow(new String[] { "0", "fragment1" });
		dataCompareTable.addRow(new String[] { "0", "fragment2" });
		dataCompareTable.addRow(new String[] { "0", "fragment3" });
		dataCompareTable.addRow(new String[] { "1", "fragment1" });
		dataCompareTable.addRow(new String[] { "1", "fragment2" });
		dataCompareTable.addRow(new String[] { "1", "fragment3" });

		ComparisonUtils.compareTables(exTable, dataCompareTable, report);

		hawq.analyze(exTable);
	}

	/**
	 * Create PXF external readable table using external plugins for Fragmenter,
	 * Accessor , Resolver and Analyzer.
	 * 
	 * @throws Exception
	 */
	@Test
	public void writableTableExternalPlugins() throws Exception {

		/**
		 * gphdfs_in
		 */
		WritableExternalTable exTable = new WritableExternalTable("extens_write", new String[] {
				"t1 text",
				"t2 text" }, "regression_location", "CUSTOM");

		exTable.setAccessor("DummyAccessor");
		exTable.setResolver("DummyResolver");
		exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
		exTable.setFormatter("pxfwritable_export");

		hawq.createTableAndVerify(exTable);

		Table dataTable = new Table("dataTable", null);

		dataTable.addRow(new String[] { "something", "big" });
		dataTable.addRow(new String[] { "is", "going" });
		dataTable.addRow(new String[] { "to", "happen" });

		hawq.insertData(dataTable, exTable);
	}

	@Test
	public void credentialsGUCsTransferredToFragmenter() throws Exception {

		ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
				"num1 integer",
				"t1 text",
				"num2 integer" }, "regression_location", "CUSTOM");

		exTable.setFragmenter("FaultyGUCFragmenter");
		exTable.setAccessor("DummyAccessor");
		exTable.setResolver("DummyResolver");
		exTable.setAnalyzer("DummyAnalyzer");
		exTable.setFormatter("pxfwritable_import");

		hawq.runQuery("SET pxf_remote_service_login = 'mommy'");
		hawq.runQuery("SET pxf_remote_service_secret = 'daddy'");
		hawq.createTableAndVerify(exTable);
		try {
			hawq.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("FaultyGUCFragmenter: login mommy secret daddy", null), true);
		}
	}

	@Test
	public void credentialsGUCsTransferredToAccessor() throws Exception {

		ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
				"num1 integer",
				"t1 text",
				"num2 integer" }, "regression_location", "CUSTOM");

		exTable.setFragmenter("DummyFragmenter");
		exTable.setAccessor("FaultyGUCAccessor");
		exTable.setResolver("DummyResolver");
		exTable.setAnalyzer("DummyAnalyzer");
		exTable.setFormatter("pxfwritable_import");

		hawq.runQuery("SET pxf_remote_service_login = 'mommy'");
		hawq.runQuery("SET pxf_remote_service_secret = 'daddy'");
		hawq.createTableAndVerify(exTable);
		try {
			hawq.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("FaultyGUCAccessor: login mommy secret daddy", null), true);
		}
	}

	@Test
	public void emptyCredentialsGUCsTransferredAsNull() throws Exception {

		ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
				"num1 integer",
				"t1 text",
				"num2 integer" }, "regression_location", "CUSTOM");

		exTable.setFragmenter("DummyFragmenter");
		exTable.setAccessor("FaultyGUCAccessor");
		exTable.setResolver("DummyResolver");
		exTable.setAnalyzer("DummyAnalyzer");
		exTable.setFormatter("pxfwritable_import");

		hawq.runQuery("SET pxf_remote_service_login = ''");
		hawq.runQuery("SET pxf_remote_service_secret = ''");
		hawq.createTableAndVerify(exTable);
		try {
			hawq.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("FaultyGUCAccessor: login null secret null", null), true);
		}
	}

	@Test
	public void defaultCredentialsGUCsTransferredAsNull() throws Exception {

		ReadableExternalTable exTable = new ReadableExternalTable("extens", new String[] {
				"num1 integer",
				"t1 text",
				"num2 integer" }, "regression_location", "CUSTOM");

		exTable.setFragmenter("DummyFragmenter");
		exTable.setAccessor("FaultyGUCAccessor");
		exTable.setResolver("DummyResolver");
		exTable.setAnalyzer("DummyAnalyzer");
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);
		try {
			hawq.queryResults(exTable, "SELECT num1, t1 FROM " + exTable.getName() + " ORDER BY num1, t1");
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("FaultyGUCAccessor: login null secret null", null), true);
		}
	}

	@After
	public void tearDown() throws Exception {
		hawq.runQuery("SET pxf_remote_service_login = ''");
		hawq.runQuery("SET pxf_remote_service_secret = ''");
	}
}
