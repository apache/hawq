package com.pxf.tests.basic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import com.pivotal.pxfauto.infra.structures.tables.basic.Table;
import com.pivotal.pxfauto.infra.structures.tables.pxf.ReadableExternalTable;
import com.pivotal.pxfauto.infra.structures.tables.pxf.WritableExternalTable;
import com.pivotal.pxfauto.infra.structures.tables.utils.TableFactory;
import com.pivotal.pxfauto.infra.utils.exception.ExceptionUtils;
import com.pivotal.pxfauto.infra.utils.fileformats.FileFormatsUtils;
import com.pivotal.pxfauto.infra.utils.jsystem.report.ReportUtils;
import com.pivotal.pxfauto.infra.utils.tables.ComparisonUtils;
import com.pxf.tests.dataprepares.sequence.CustomSequencePreparer;
import com.pxf.tests.dataprepares.writable.WritableTextPreparer;
import com.pxf.tests.testcases.PxfTestCase;

/**
 * PXF using Writable External Tables with different types of data
 */
public class PxfHdfsWritableRegression extends PxfTestCase {

	WritableExternalTable weTable;
	ReadableExternalTable reTable;
	static String[] syntaxFields = new String[] { "a int", "b text", "c bytea" };
	static String[][] textData = new String[][] {
			{ "first", "1", "11" },
			{ "second", "2", "22" },
			{ "third", "3", "33" },
			{ "fourth", "4", "44" },
			{ "fifth", "5", "55" },
			{ "sixth", "6", "66" },
			{ "seventh", "7", "77" },
			{ "eighth", "8", "88" },
			{ "ninth", "9", "99" },
			{ "tenth", "10", "1010" },
			{ "eleventh", "11", "1111" },
			{ "twlefth", "12", "1212" },
			{ "thirteenth", "13", "1313" },
			{ "fourteenth", "14", "1414" },
			{ "fifteenth", "15", "1515" } };
	static String[][] players = new String[][] {
			{ "4", "H Waldman", "P" },
			{ "6", "Papi Turgeman", "S" },
			{ "13", "Radisav Ćurčić", "C" },
			{ "11", "Derrick Hamilton", "F" },
			{ "15", "Kenny Williams", "F" },
			{ "8", "Doron Shefa", "S" },
			{ "13", "Erez Katz", "P" } };
	static String[] customWritableFields = new String[] {
			"tmp1  timestamp",
			"num1  integer",
			"num2  integer",
			"num3  integer",
			"num4  integer",
			"t1    text",
			"t2    text",
			"t3    text",
			"t4    text",
			"t5    text",
			"t6    text",
			"dub1  double precision",
			"dub2  double precision",
			"dub3  double precision",
			"ft1   real",
			"ft2   real",
			"ft3   real",
			"ln1   bigint",
			"ln2   bigint",
			"ln3   bigint",
			"bt    bytea",
			"bool1 boolean",
			"bool2 boolean",
			"bool3 boolean",
			"short1 smallint",
			"short2 smallint",
			"short3 smallint",
			"short4 smallint",
			"short5 smallint" };
	String hdfsDir;

	/**
	 * Create writable external table with accessor and resolver
	 * 
	 * @throws Exception
	 */
	@Test
	public void syntaxValidation() throws Exception {

		// String name, String[] fields, String path, String format
		weTable = new WritableExternalTable("pxf_out", syntaxFields, hdfsWorkingFolder + "/writable", "CUSTOM");

		weTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		weTable.setResolver("com.pivotal.pxf.plugins.hdfs.AvroResolver");
		weTable.setDataSchema("MySchema");
		weTable.setFormatter("pxfwritable_export");

		hawq.createTableAndVerify(weTable);
	}

	/**
	 * Create writable table with no accessor and resolver. Should fail and
	 * throw the right message.
	 * 
	 * @throws Exception
	 */
	@Test
	public void neagtiveMissingParameter() throws Exception {

		weTable = new WritableExternalTable("pxf_out1", syntaxFields, hdfsWorkingFolder + "/writable", "CUSTOM");
		weTable.setFormatter("pxfwritable_export");
		weTable.setUserParameters(new String[] { "someuseropt=someuserval" });

		try {
			hawq.createTableAndVerify(weTable);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("ERROR: Invalid URI pxf://" + hawq.getHost() + ":50070/" + weTable.getPath() + "?someuseropt=someuserval: PROFILE or ACCESSOR and RESOLVER option(s) missing", null), false);
		}
	}

	/**
	 * Create writable table with no parameters. Should fail and throw the right
	 * message.
	 * 
	 * @throws Exception
	 */
	@Test
	public void neagtiveNoParameters() throws Exception {

		weTable = new WritableExternalTable("pxf_out2", syntaxFields, hdfsWorkingFolder + "/writable/*", "CUSTOM");
		weTable.setFormatter("pxfwritable_export");

		String createQuery = weTable.constructCreateStmt();
		createQuery = createQuery.replace("?", "");

		try {
			hawq.runQuery(createQuery);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("ERROR: Invalid URI pxf://" + hawq.getHost() + ":50070/" + weTable.getPath() + ": missing options section", null), false);
		}
	}

	/**
	 * Create writable table, insert data to it, check that data is written to
	 * HDFS, then read it with Readable Table.
	 * 
	 * @throws Exception
	 */
	@Test
	public void textInsert() throws Exception {

		String hdfsDir = hdfsWorkingFolder + "/writable/wrtext";

		WritableExternalTable writableTable = TableFactory.getPxfWritableTextTable("wrtext", new String[] {
				"s1 text",
				"n1 int",
				"n2 int" }, hdfsDir, ",");

		hawq.createTableAndVerify(writableTable);
		Table sudoTable = new Table("sudoTable", null);
		Table smallTable = new Table("smallTable", null);

		ReportUtils.report(report, getClass(), "insert data with multiple insert commands");
		for (int i = 0; i < 14; i = i + 2) {
			smallTable.addRow(textData[i]);
			smallTable.addRow(textData[i + 1]);
			if (i == 12) {
				smallTable.addRow(textData[i + 2]);
			}
			sudoTable.appendRows(smallTable);

			hawq.insertData(smallTable, writableTable);

			smallTable.setData(null);
		}

		Assert.assertNotEquals(hdfs.listSize(hdfsDir), 0);

		ReadableExternalTable readableTable = TableFactory.getPxfReadableTextTable("retext", new String[] {
				"s1 text",
				"n1 int",
				"n2 int" }, hdfsDir, ",");

		hawq.createTableAndVerify(readableTable);
		hawq.queryResults(readableTable, "SELECT * FROM " + readableTable.getName() + " ORDER BY n1");

		ComparisonUtils.compareTables(readableTable, sudoTable, report);
	}

	/**
	 * insert to writable table from readable table
	 * 
	 * @throws Exception
	 */
	@Test
	public void textInsertFromRead() throws Exception {

		String hdfsDir = hdfsWorkingFolder + "/writable/wrtext";

		hdfs.removeDirectory(hdfsDir);

		ReportUtils.report(report, getClass(), "create writable table");
		WritableExternalTable writableTable = TableFactory.getPxfWritableTextTable("wrtext", new String[] {
				"s1 text",
				"n1 int",
				"n2 int" }, hdfsDir, ",");

		hawq.createTableAndVerify(writableTable);
		Table sudoTable = new Table("sudoTable", null);

		for (int i = 0; i < textData.length; i++) {
			sudoTable.addRow(textData[i]);
		}
		hawq.insertData(sudoTable, writableTable);

		Assert.assertNotEquals(hdfs.listSize(hdfsDir), 0);

		ReportUtils.report(report, getClass(), "create read table");
		ReadableExternalTable readableTable = TableFactory.getPxfReadableTextTable("retext", new String[] {
				"s1 text",
				"n1 int",
				"n2 int" }, hdfsDir, ",");

		hawq.createTableAndVerify(readableTable);

		ReportUtils.report(report, PxfHdfsWritableRegression.class, "insert from read table");
		hawq.runQuery("INSERT INTO " + writableTable.getName() + " SELECT * FROM " + readableTable.getName() + " WHERE n1<=10", false);
		hawq.runQuery("INSERT INTO " + writableTable.getName() + " SELECT * FROM " + readableTable.getName() + " WHERE n2<100", false);

		ReportUtils.report(report, getClass(), "create compare data");
		sudoTable.setData(null);
		// all rows from 1 to 9 will appear 4 times in the table
		for (int i = 0; i < 9; ++i) {
			for (int j = 0; j < 4; ++j) {
				sudoTable.addRow(textData[i]);
			}
		}
		// row 10 will appear 2 times
		sudoTable.addRow(textData[9]);
		// rows 11 to 15 will appear 1 time
		for (int i = 9; i < 15; ++i) {
			sudoTable.addRow(textData[i]);
		}

		hawq.queryResults(readableTable, "SELECT * FROM " + readableTable.getName() + " ORDER BY n1");
		ComparisonUtils.compareTables(readableTable, sudoTable, report);
	}

	/**
	 * Create writable table with Gzip codec, insert data to it using copy, then
	 * read it from HDFS using Readable Table.
	 * 
	 * @throws Exception
	 */
	@Test
	public void gzipText() throws Exception {

		String[] fields = new String[] { "s1 text", "n1 int", "n2 int" };

		createDir("wrgzip");

		WritableExternalTable writableTable = TableFactory.getPxfWritableGzipTable("wrgzip", fields, hdfsDir, ",");
		hawq.createTableAndVerify(writableTable);

		Table dataTable = new Table("gzip", null);
		FileFormatsUtils.prepareData(new WritableTextPreparer(), 3000, dataTable);

		ReportUtils.report(report, getClass(), "copy into writable table from stdin");
		hawq.copyFromStdin(dataTable, writableTable, ",", false);
		ReportUtils.report(report, getClass(), "copy into writable table from stdin csv");
		hawq.copyFromStdin(dataTable, writableTable, null, true);

		dataTable.pumpUpTableData(2);

		Assert.assertNotEquals(hdfs.listSize(hdfsDir), 0);

		createReadableAndCompare(fields, dataTable);
	}

	/**
	 * Create writable table with Default codec, insert data to it, then read it
	 * from HDFS using Readable Table.
	 * 
	 * @throws Exception
	 */
	@Test
	public void deflateText() throws Exception {

		String[] fields = new String[] { "s1 text", "n1 int", "n2 int" };

		createDir("wrdeflate");

		WritableExternalTable writableTable = TableFactory.getPxfWritableTextTable("wrdeflate", fields, hdfsDir, ",");
		writableTable.setCompressionCodec("org.apache.hadoop.io.compress.DefaultCodec");
		hawq.createTableAndVerify(writableTable);

		Table dataTable = new Table("default", null);
		FileFormatsUtils.prepareData(new WritableTextPreparer(), 500, dataTable);

		ReportUtils.report(report, getClass(), "insert into writable table");
		hawq.insertData(dataTable, writableTable);

		Assert.assertNotEquals(hdfs.listSize(hdfsDir), 0);

		createReadableAndCompare(fields, dataTable);
	}

	/**
	 * Create writable table with BZip2 codec, insert data to it using copy,
	 * than read it from HDFS using Readable Table.
	 * 
	 * @throws Exception
	 */
	@Test
	public void bzip2Text() throws Exception {

		String[] fields = new String[] { "s1 text", "n1 int", "n2 int" };

		createDir("bzip2");

		weTable = TableFactory.getPxfWritableTextTable("wrbzip2", fields, hdfsDir, ",");
		weTable.setCompressionCodec("org.apache.hadoop.io.compress.BZip2Codec");
		hawq.createTableAndVerify(weTable);

		Table dataTable = new Table("bzip2", null);
		FileFormatsUtils.prepareData(new WritableTextPreparer(), 500, dataTable);

		ReportUtils.report(report, getClass(), "create temp table");
		Table tempTable = new Table("data_for_bzip2", null);
		hawq.runQueryWithExpectedWarning("CREATE TEMP TABLE " + tempTable.getName() + " (LIKE " + weTable.getName() + ")", "Table doesn't have 'distributed by' clause, defaulting to distribution columns from LIKE table", false);
		ReportUtils.report(report, getClass(), "insert data into temp table");
		hawq.insertData(dataTable, tempTable);

		ReportUtils.report(report, getClass(), "insert into writable table from temp table");
		hawq.runQuery("INSERT INTO " + weTable.getName() + " SELECT * FROM " + tempTable.getName());

		Assert.assertNotEquals(hdfs.listSize(hdfsDir), 0);

		createReadableAndCompare(fields, dataTable);
	}

	/**
	 * sequence file write and read - all compression combinations
	 * 
	 * @throws Exception
	 */
	@Test
	public void sequence() throws Exception {

		String schema = "CustomWritable";
		String[] codecs = new String[] {
				null,
				/**
				 * Gzip is not supported in Mac and RHEL 5 [JIRA:GPSQL-1179]
				 * 
				 * "org.apache.hadoop.io.compress.GzipCodec",
				 **/
				"org.apache.hadoop.io.compress.DefaultCodec",
				"org.apache.hadoop.io.compress.BZip2Codec" };
		String[][] userParams = new String[][] {
				null,
				{ "COMPRESSION_TYPE=RECORD" },
				{ "COMPRESSION_TYPE=BLOCK" } };

		ReportUtils.report(report, getClass(), "create data file");
		Table sudoDataTable = new Table("dataTable", null);
		FileFormatsUtils.prepareData(new CustomSequencePreparer(), 100, sudoDataTable);

		File path = new File(loaclTempFolder + "/customwritable_data.txt");

		createCustomWritableDataFile(sudoDataTable, path, false);

		ReportUtils.report(report, getClass(), "Run sequence file test with various parameters");

		int testNum = 1;
		for (String codec : codecs) {
			for (String[] userParam : userParams) {

				String testDescription = "sequence test #" + testNum + ": with " + ((codec == null) ? "no" : codec) + " codec" + ". user params:" + ((userParam == null) ? "null" : ArrayUtils.toString(userParam));
				ReportUtils.startLevel(report, getClass(), testDescription);
				hdfsDir = hdfsWorkingFolder + "/writable/wrcustom" + testNum;
				weTable = TableFactory.getPxfWritableSequenceTable("wrseq" + testNum, customWritableFields, hdfsDir, schema);
				weTable.setCompressionCodec(codec);
				weTable.setUserParameters(userParam);

				reTable = TableFactory.getPxfReadableSequenceTable("readseq" + testNum, customWritableFields, hdfsDir, schema);
				runSequenceTest(sudoDataTable, path);
				ReportUtils.stopLevel(report);

				++testNum;
			}
		}
	}

	/**
	 * insert into writable table with wrong port
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeWrongPort() throws Exception {
		weTable = new WritableExternalTable("wr_wrong_port", new String[] {
				"t1 text",
				"a1 integer" }, hdfsWorkingFolder + "/writable/err", "TEXT");
		weTable.setDelimiter(",");
		weTable.setAccessor("TextFileWAccessor");
		weTable.setResolver("TextWResolver");
		weTable.setPort("12345");

		hawq.createTableAndVerify(weTable);

		Table dataTable = new Table("data", null);
		dataTable.addRow(new String[] { "first", "1" });
		dataTable.addRow(new String[] { "second", "2" });
		dataTable.addRow(new String[] { "third", "3" });

		try {
			hawq.insertData(dataTable, weTable);
		} catch (PSQLException e) {
			ExceptionUtils.validate(report, e, new PSQLException("ERROR: remote component error \\(0\\): " + "(Failed connect to " + weTable.getHostname() + ":12345; Connection refused|couldn't connect to host).*?", null), true);
		}
	}

	/**
	 * insert into table with bad host name
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeBadHost() throws Exception {
		weTable = new WritableExternalTable("wr_host_err", new String[] {
				"t1 text",
				"a1 integer" }, hdfsWorkingFolder + "/writable/err", "TEXT");
		weTable.setDelimiter(",");
		weTable.setAccessor("TextFileWAccessor");
		weTable.setResolver("TextWResolver");
		weTable.setHostname("badhostname");

		hawq.createTableAndVerify(weTable);

		Table dataTable = new Table("data", null);
		dataTable.addRow(new String[] { "first", "1" });
		dataTable.addRow(new String[] { "second", "2" });
		dataTable.addRow(new String[] { "third", "3" });

		try {
			hawq.insertData(dataTable, weTable);
		} catch (PSQLException e) {
			ExceptionUtils.validate(report, e, new PSQLException("ERROR: remote component error \\(0\\): " + "Couldn't resolve host 'badhostname'.*?", null), true);
		}
	}

	/**
	 * Test circle type converted to text and back
	 * 
	 * @throws Exception
	 */
	@Test
	public void circleType() throws Exception {

		String[] fields = new String[] { "a1 integer", "c1 circle" };
		String hdfsDir = hdfsWorkingFolder + "/writable/circle";

		weTable = new WritableExternalTable("wr_circle", fields, hdfsDir, "CUSTOM");
		weTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		weTable.setResolver("com.pivotal.pxf.plugins.hdfs.WritableResolver");
		weTable.setDataSchema("CustomWritableWithCircle");
		weTable.setFormatter("pxfwritable_export");

		hawq.createTableAndVerify(weTable);

		ReportUtils.report(report, getClass(), "load circle data");

		Table dataTable = new Table("circle", null);
		dataTable.addRow(new String[] { "1", "<(3,3),9>" });
		dataTable.addRow(new String[] { "2", "<(4,4),16>" });

		hawq.insertData(dataTable, weTable);

		ReportUtils.report(report, getClass(), "read circle data");

		reTable = new ReadableExternalTable("read_circle", fields, hdfsDir, "CUSTOM");
		reTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		reTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		reTable.setResolver("com.pivotal.pxf.plugins.hdfs.WritableResolver");
		reTable.setDataSchema("CustomWritableWithCircle");
		reTable.setFormatter("pxfwritable_import");
		hawq.createTableAndVerify(reTable);

		hawq.queryResults(reTable, "SELECT * FROM " + reTable.getName() + " ORDER BY a1");

		ComparisonUtils.compareTables(reTable, dataTable, report);
	}

	/**
	 * Test unsupported type in writable resolver -- negative
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeCharType() throws Exception {

		weTable = new WritableExternalTable("wr_host_err", new String[] {
				"a1 integer",
				"c1 char" }, hdfsWorkingFolder + "/writable/err", "CUSTOM");
		weTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		weTable.setResolver("com.pivotal.pxf.plugins.hdfs.WritableResolver");
		weTable.setDataSchema("CustomWritableWithChar");
		weTable.setFormatter("pxfwritable_export");

		hawq.createTableAndVerify(weTable);

		Table dataTable = new Table("data", null);
		dataTable.addRow(new String[] { "100", "a" });
		dataTable.addRow(new String[] { "1000", "b" });

		try {
			hawq.insertData(dataTable, weTable);
		} catch (PSQLException e) {

			ExceptionUtils.validate(report, e, new PSQLException("ERROR: remote component error \\(500\\) from " + "'.*?': " + "Problem accessing /gpdb/v(\\d+)/Writable/stream. Reason:(\\s+)" + "Server Error(\\s+)Caused by:(\\s+)com.pivotal.pxf.api.UnsupportedTypeException: Type char is not supported by GPDBWritable.*?", null), true);
		}
	}

	/**
	 * Test ANALYZE on writable table with no analyzer
	 * 
	 * @throws Exception
	 */
	@Test
	public void analyzeWithoutAnalyzer() throws Exception {

		String[] fields = new String[] { "s1 text", "n1 integer", "n2 integer" };

		weTable = new WritableExternalTable("writable_analyze_no_analyzer", fields, hdfsWorkingFolder + "/writable/analyze/noanalyzer", "TEXT");
		weTable.setProfile("HdfsTextSimple");
		ReportUtils.report(report, getClass(), "run analyze on writable table with analyzer");
		runAnalyzeTest();
	}

	/**
	 * Test ANALYZE on writable table with analyzer
	 * 
	 * @throws Exception
	 */
	@Test
	public void analyzeWithAnalyzer() throws Exception {

		String[] fields = new String[] { "s1 text", "n1 integer", "n2 integer" };

		weTable = new WritableExternalTable("writable_analyze_with_analyzer", fields, hdfsWorkingFolder + "/writable/analyze/analyzer", "TEXT");
		weTable.setAccessor("com.pivotal.pxf.plugins.hdfs.LineBreakAccessor");
		weTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");

		ReportUtils.report(report, getClass(), "run analyze on writable table without analyzer");

		runAnalyzeTest();
	}

	/**
	 * Test COMPRESSION_TYPE = NONE -- negative
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeCompressionTypeNone() throws Exception {

		String[] fields = new String[] { "a1 integer", "c1 char" };

		weTable = TableFactory.getPxfWritableSequenceTable("compress_type_none", fields, hdfsWorkingFolder + "/writable/err", "SomeClass");
		weTable.setUserParameters(new String[] { "COMPRESSION_TYPE=NONE" });

		hawq.createTableAndVerify(weTable);

		Table dataTable = new Table("data", null);
		dataTable.addRow(new String[] { "100", "a" });
		dataTable.addRow(new String[] { "1000", "b" });

		try {
			hawq.insertData(dataTable, weTable);
		} catch (PSQLException e) {
			ExceptionUtils.validate(report, e, new PSQLException("ERROR: remote component error \\(500\\) from " + "'.*?': " + "Problem accessing /gpdb/v(\\d+)/Writable/stream. Reason:(\\s+)" + "Server Error(\\s+)Caused by:(\\s+)java.lang.IllegalArgumentException: " + "Illegal compression type 'NONE'\\. For disabling compression remove COMPRESSION_CODEC parameter\\..*?", null), true);
		}
	}

	/**
	 * Test CSV format
	 * 
	 * @throws Exception
	 */
	@Test
	public void csv() throws Exception {

		String[] fields = new String[] {
				"number int",
				"name text",
				"position char" };
		String hdfsDir = hdfsWorkingFolder + "/writable/csv";

		weTable = new WritableExternalTable("writable_csv", fields, hdfsDir, "CSV");
		weTable.setProfile("HdfsTextSimple");

		hawq.createTableAndVerify(weTable);

		Table dataTable = new Table("dataTable", null);
		for (int i = 0; i < 5; ++i) {
			dataTable.addRow(players[i]);
		}
		hawq.insertData(dataTable, weTable);

		reTable = new ReadableExternalTable("readable_csv", fields, hdfsDir, "CSV");
		reTable.setProfile("HdfsTextSimple");

		hawq.createTableAndVerify(reTable);

		hawq.queryResults(reTable, "SELECT * FROM " + reTable.getName() + " ORDER BY number");

		ReportUtils.report(report, getClass(), "reorder data table");
		List<List<String>> rows = dataTable.getData();
		List<String> row = rows.get(2);
		rows.set(2, rows.get(3));
		rows.set(3, row);

		ComparisonUtils.compareTables(reTable, dataTable, report);
	}

	/**
	 * Test write & read with THREAD-SAFE parameter set to FALSE
	 * 
	 * @throws Exception
	 */
	@Test
	public void threadSafe() throws Exception {
		String[] fields = new String[] {
				"number int",
				"name text",
				"position char" };
		String hdfsDir = hdfsWorkingFolder + "/writable/threadsafe";

		weTable = new WritableExternalTable("writable_threadsafe", fields, hdfsDir, "CSV");
		weTable.setProfile("HdfsTextSimple");
		weTable.setUserParameters(new String[] { "THREAD-SAFE=FALSE" });

		hawq.createTableAndVerify(weTable);

		Table dataTable = new Table("dataTable", null);
		for (int i = 0; i < 5; ++i) {
			dataTable.addRow(players[i]);
		}
		hawq.insertData(dataTable, weTable);

		Table smallTable = new Table("smallTable", null);
		smallTable.addRow(players[5]);
		smallTable.addRow(players[6]);
		hawq.insertData(smallTable, weTable);

		reTable = new ReadableExternalTable("readable_threadsafe", fields, hdfsDir, "CSV");
		reTable.setProfile("HdfsTextSimple");
		reTable.setUserParameters(new String[] { "THREAD-SAFE=FALSE" });

		hawq.createTableAndVerify(reTable);

		hawq.queryResults(reTable, "SELECT * FROM " + reTable.getName() + " ORDER BY number");

		ReportUtils.report(report, getClass(), "reorder data table");
		dataTable.setData(null);
		dataTable.addRow(players[0]);
		dataTable.addRow(players[1]);
		dataTable.addRow(players[5]);
		dataTable.addRow(players[3]);
		dataTable.addRow(players[2]);
		dataTable.addRow(players[6]);
		dataTable.addRow(players[4]);

		ComparisonUtils.compareTables(reTable, dataTable, report);
	}

	/**
	 * Test recordkey for sequence file - recordkey of type text
	 * 
	 * @throws Exception
	 */
	@Test
	public void recordkeyTextType() throws Exception {

		String[] fields = new String[customWritableFields.length + 1];
		fields[0] = "recordkey text";
		System.arraycopy(customWritableFields, 0, fields, 1, customWritableFields.length);

		hdfsDir = hdfsWorkingFolder + "/writable/recordkey_text";

		weTable = TableFactory.getPxfWritableSequenceTable("writable_recordkey_text", fields, hdfsDir, "CustomWritable");

		hawq.createTableAndVerify(weTable);

		ReportUtils.report(report, getClass(), "create data file");
		Table dataTable = new Table("dataTable", null);
		FileFormatsUtils.prepareData(new CustomSequencePreparer(), 50, dataTable);

		File path = new File(loaclTempFolder + "/customwritable_recordkey_data.txt");
		createCustomWritableDataFile(dataTable, path, true);

		hawq.copyFromFile(weTable, path, null, false);

		reTable = TableFactory.getPxfReadableSequenceTable("readable_recordkey_text", fields, hdfsDir, "CustomWritable");
		hawq.createTableAndVerify(reTable);

		hawq.queryResults(reTable, "SELECT * FROM " + reTable.getName() + " ORDER BY num1 LIMIT 10");

		List<List<String>> rows = dataTable.getData();
		dataTable.setData(rows.subList(0, 10));

		ComparisonUtils.compareTables(reTable, dataTable, report);
	}

	/**
	 * Test recordkey for sequence file - recordkey of type int One row will
	 * fail - it has a text value
	 * 
	 * @throws Exception
	 */
	@Test
	public void recordkeyIntType() throws Exception {

		String[] fields = new String[customWritableFields.length + 1];
		fields[0] = "recordkey int";
		System.arraycopy(customWritableFields, 0, fields, 1, customWritableFields.length);

		hdfsDir = hdfsWorkingFolder + "/writable/recordkey_int";

		weTable = TableFactory.getPxfWritableSequenceTable("writable_recordkey_int", fields, hdfsDir, "CustomWritable");

		hawq.createTableAndVerify(weTable);

		ReportUtils.report(report, getClass(), "create data file");
		Table dataTable = new Table("dataTable", null);
		FileFormatsUtils.prepareData(new CustomSequencePreparer(), 50, dataTable);

		File path = new File(loaclTempFolder + "/customwritable_recordkey_data.txt");
		createCustomWritableDataFile(dataTable, path, true);

		ReportUtils.report(report, getClass(), "copy data from file to table");
		String copyCmd = "COPY " + weTable.getName() + " FROM '" + path.getAbsolutePath() + "'" + "SEGMENT REJECT LIMIT 5 ROWS;";
		String noticeMsg = "Found 1 data formatting errors (1 or more input rows). Rejected related input data.";
		hawq.runQueryWithExpectedWarning(copyCmd, noticeMsg, false);

		reTable = TableFactory.getPxfReadableSequenceTable("readable_recordkey_int", fields, hdfsDir, "CustomWritable");
		hawq.createTableAndVerify(reTable);

		hawq.queryResults(reTable, "SELECT * FROM " + reTable.getName() + " ORDER BY num1");

		List<List<String>> rows = dataTable.getData();
		rows.remove(1);

		ComparisonUtils.compareTables(reTable, dataTable, report);
	}

	/*
	 * Helper functions
	 */

	private void runAnalyzeTest() throws Exception {
		setPxfStatCollection(true);

		hawq.createTableAndVerify(weTable);

		analyzeAndVerifyDefaultValues(weTable.getName());

		ReportUtils.report(report, getClass(), "run analyze on writable table after insert data");
		hawq.runQuery("INSERT INTO " + weTable.getName() + " VALUES ('nothing', 0, 0), ('will', 0, 0), ('happen', 0, 0);");
		analyzeAndVerifyDefaultValues(weTable.getName());

		ReportUtils.report(report, getClass(), "run analyze on writable table with GUC off");
		setPxfStatCollection(false);
		analyzeAndVerifyDefaultValues(weTable.getName());
		setPxfStatCollection(true);
	}

	private void setPxfStatCollection(boolean state) throws Exception {

		String newState = state ? "true" : "false";

		ReportUtils.report(report, getClass(), "set pxf_enable_stat_collection to " + newState);
		hawq.runQuery("SET pxf_enable_stat_collection = " + newState + ";");
	}

	private void analyzeAndVerifyDefaultValues(String tableName)
			throws Exception {
		ReportUtils.startLevel(report, getClass(), "verify analyze on " + tableName + " table results in default values");

		hawq.runQuery("ANALYZE " + tableName);

		Table analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "	SELECT COUNT(*) FROM pg_class WHERE relname = '" + tableName + "' AND relpages = 1 AND reltuples = 0");

		Table sudoResults = new Table("sudoResults", null);

		sudoResults.addRow(new String[] { "1" });

		ComparisonUtils.compareTables(analyzeResults, sudoResults, report);

		ReportUtils.stopLevel(report);
	}

	private void createDir(String name) throws Exception {

		hdfsDir = hdfsWorkingFolder + "/writable/" + name;

		hdfs.removeDirectory(hdfsDir);
	}

	private void createReadableAndCompare(String[] fields, Table dataTable)
			throws Exception {

		ReadableExternalTable readableTable = TableFactory.getPxfReadableTextTable("readgzip", fields, hdfsDir, ",");

		hawq.createTableAndVerify(readableTable);

		hawq.queryResults(readableTable, "SELECT * FROM " + readableTable.getName() + " ORDER BY n1");

		ComparisonUtils.compareTables(readableTable, dataTable, report);
	}

	/**
	 * create data file based on CustomWritable schema. data is written from
	 * sudoDataTable and to path file.
	 * 
	 * @param sudoDataTable
	 * @param path
	 * @param recordkey
	 *            add recordkey data to the beginning of each row
	 * @throws IOException
	 */
	private void createCustomWritableDataFile(Table sudoDataTable, File path, boolean recordkey)
			throws IOException {

		List<List<String>> data = sudoDataTable.getData();

		Assert.assertTrue(path.createNewFile());

		FileWriter fstream = new FileWriter(path);
		BufferedWriter out = new BufferedWriter(fstream);

		int i = 0;
		for (List<String> row : data) {
			if (recordkey) {
				String record = "0000" + ++i;
				if (i == 2) {
					record = "NotANumber";
				}
				row.add(0, record);
			}
			String formatted = StringUtils.join(row, "\t") + "\n";
			out.write(formatted);
		}

		if (out != null) {
			out.close();
		}
	}

	private void runSequenceTest(Table dataTable, File path) throws Exception {
		hawq.createTableAndVerify(weTable);
		hawq.copyFromFile(weTable, path, null, false);
		Assert.assertNotEquals(hdfs.listSize(hdfsDir), 0);
		hawq.createTableAndVerify(reTable);
		hawq.queryResults(reTable, "SELECT * FROM " + reTable.getName() + " ORDER BY num1");
		ComparisonUtils.compareTables(reTable, dataTable, report);
	}
}
