package com.pxf.tests.basic;

import java.io.File;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import com.pivotal.pxfauto.infra.common.ShellSystemObject;
import com.pivotal.pxfauto.infra.fileformats.IAvroSchema;
import com.pivotal.pxfauto.infra.structures.tables.basic.Table;
import com.pivotal.pxfauto.infra.structures.tables.pxf.ReadableExternalTable;
import com.pivotal.pxfauto.infra.structures.tables.utils.TableFactory;
import com.pivotal.pxfauto.infra.utils.exception.ExceptionUtils;
import com.pivotal.pxfauto.infra.utils.fileformats.FileFormatsUtils;
import com.pivotal.pxfauto.infra.utils.jsystem.report.ReportUtils;
import com.pivotal.pxfauto.infra.utils.regex.RegexUtils;
import com.pivotal.pxfauto.infra.utils.tables.ComparisonUtils;
import com.pxf.tests.dataprepares.avro.CustomAvroPreparer;
import com.pxf.tests.dataprepares.sequence.CustomAvroInSequencePreparer;
import com.pxf.tests.dataprepares.sequence.CustomSequencePreparer;
import com.pxf.tests.dataprepares.text.CustomTextPreparer;
import com.pxf.tests.dataprepares.text.QuotedLineTextPreparer;
import com.pxf.tests.testcases.PxfTestCase;

/**
 * PXF on HDFS Regression tests
 */
public class PxfHdfsRegression extends PxfTestCase {

	ReadableExternalTable exTable;

	/**
	 * General Table creation Validations with Fragmenter, Accessor and Resolver
	 * 
	 * @throws Exception
	 */
	@Test
	public void syntaxValidations() throws Exception {

		/**
		 * gphdfs_in
		 */
		exTable = new ReadableExternalTable("gphdfs_in", new String[] {
				"a int",
				"b text",
				"c bytea" }, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

		exTable.setFragmenter("xfrag");
		exTable.setAccessor("xacc");
		exTable.setResolver("xres");
		exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		/**
		 * gphdfs_in1
		 */
		exTable.setName("gphdfs_in1");
		exTable.setPath(hdfsWorkingFolder);
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugin.hdfs.AvroResolver");
		exTable.setDataSchema("MySchema");

		exTable.setUserParameters(null);

		hawq.createTableAndVerify(exTable);
	}

	/**
	 * Create Table with no Fragmenter, Accessor and Resolver. Should fail and
	 * throw the right message.
	 * 
	 * @throws Exception
	 */
	@Test
	public void neagtiveNoFragmenterNoAccossorNoResolver() throws Exception {

		exTable = new ReadableExternalTable("gphdfs_in2", new String[] {
				"a int",
				"b text",
				"c bytea" }, (hdfsWorkingFolder + "/*"), "CUSTOM");

		exTable.setFormatter("pxfwritable_import");

		try {

			hawq.createTableAndVerify(exTable);

		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("ERROR: Invalid URI pxf://" + hawq.getHost() + ":50070/" + exTable.getPath() + "?: invalid option after '?'", null), false);
		}
	}

	/**
	 * Create Table with no Fragmenter Should fail and throw the right message.
	 * 
	 * @throws Exception
	 */
	@Test
	public void neagtiveMissingFragmenter() throws Exception {

		exTable = new ReadableExternalTable("gphdfs_in2", new String[] {
				"a int",
				"b text",
				"c bytea" }, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

		exTable.setAccessor("xacc");
		exTable.setResolver("xres");
		exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
		exTable.setFormatter("pxfwritable_import");

		try {
			hawq.createTableAndVerify(exTable);
		} catch (Exception e) {

			ExceptionUtils.validate(report, e, new PSQLException("ERROR: Invalid URI pxf://" + hawq.getHost() + ":50070/" + exTable.getPath() + "?ACCESSOR=xacc&RESOLVER=xres&someuseropt=someuserval: PROFILE or FRAGMENTER option(s) missing", null), false);
		}
	}

	/**
	 * Create Table with no Accessor Should fail and throw the right message.
	 * 
	 * @throws Exception
	 */
	@Test
	public void neagtiveMissingAccessor() throws Exception {

		exTable = new ReadableExternalTable("gphdfs_in2", new String[] {
				"a int",
				"b text",
				"c bytea" }, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

		exTable.setFragmenter("xfrag");
		exTable.setResolver("xres");
		exTable.setUserParameters(new String[] { "someuseropt=someuserval" });
		exTable.setFormatter("pxfwritable_import");

		try {
			hawq.createTableAndVerify(exTable);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("ERROR: Invalid URI pxf://" + hawq.getHost() + ":50070/" + exTable.getPath() + "?FRAGMENTER=xfrag&RESOLVER=xres&someuseropt=someuserval: PROFILE or ACCESSOR option(s) missing", null), false);
		}
	}

	/**
	 * Create Table with no Resolver. Should fail and throw the right message.
	 * 
	 * @throws Exception
	 */
	@Test
	public void neagtiveMissingResolver() throws Exception {

		exTable = new ReadableExternalTable("gphdfs_in2", new String[] {
				"a int",
				"b text",
				"c bytea" }, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

		exTable.setFragmenter("xfrag");
		exTable.setAccessor("xacc");
		exTable.setFormatter("pxfwritable_import");

		try {

			hawq.createTableAndVerify(exTable);

		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("ERROR: Invalid URI pxf://" + hawq.getHost() + ":50070/" + exTable.getPath() + "?FRAGMENTER=xfrag&ACCESSOR=xacc: PROFILE or RESOLVER option(s) missing", null), false);
		}
	}

	/**
	 * Use "com.pivotal.pxf.plugins.hdfs.fragmenters.HdfsDataFragmenter" +
	 * "LineReaderAccessor" + "StringPassResolver" and TEXT delimiter for
	 * parsing CSV file.
	 * 
	 * @throws Exception
	 */
	@Test
	public void textFormatsManyFields() throws Exception {

		String csvPath = hdfsWorkingFolder + "/text_data.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

		hdfs.writeTextFile(csvPath, dataTable.getData(), ",");

		exTable = new ReadableExternalTable("bigtext", new String[] {
				"s1 text",
				"s2 text",
				"s3 text",
				"d1 timestamp",
				"n1 int",
				"n2 int",
				"n3 int",
				"n4 int",
				"n5 int",
				"n6 int",
				"n7 int",
				"s11 text",
				"s12 text",
				"s13 text",
				"d11 timestamp",
				"n11 int",
				"n12 int",
				"n13 int",
				"n14 int",
				"n15 int",
				"n16 int",
				"n17 int" }, csvPath, "TEXT");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.LineBreakAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");
		exTable.setDelimiter(",");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM bigtext ORDER BY n1");

		ComparisonUtils.compareTables(exTable, dataTable, report);

		ReportUtils.report(report, getClass(), "single condition - remove elements from dataTable and compare");
		hawq.queryResults(exTable, "SELECT * FROM bigtext " + "WHERE n2 > 500 ORDER BY n1");

		List<List<String>> data = dataTable.getData();
		dataTable.setData(data.subList(50, 1000));

		ComparisonUtils.compareTables(exTable, dataTable, report);

		ReportUtils.report(report, getClass(), "two conditions - remove elements from dataTable and compare");
		hawq.queryResults(exTable, "SELECT * FROM bigtext " + "WHERE (n2 > 500) AND (n1 <= 60) ORDER BY n1");

		dataTable.setData(data.subList(50, 60));

		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	/**
	 * Use "com.pivotal.pxf.plugins.hdfs.fragmenters.HdfsDataFragmenter" +
	 * "LineReaderAccessor" + "StringPassResolver" along with Format("CSV")
	 * 
	 * @throws Exception
	 */
	@Test
	public void textCsvFormatOnManyFields() throws Exception {

		String csvPath = hdfsWorkingFolder + "/text_data.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

		hdfs.writeTextFile(csvPath, dataTable.getData(), ",");

		exTable = new ReadableExternalTable("bigtext", new String[] {
				"s1 text",
				"s2 text",
				"s3 text",
				"d1 timestamp",
				"n1 int",
				"n2 int",
				"n3 int",
				"n4 int",
				"n5 int",
				"n6 int",
				"n7 int",
				"s11 text",
				"s12 text",
				"s13 text",
				"d11 timestamp",
				"n11 int",
				"n12 int",
				"n13 int",
				"n14 int",
				"n15 int",
				"n16 int",
				"n17 int" }, (hdfsWorkingFolder + "/text_data.csv"), "CSV");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.LineBreakAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM  " + exTable.getName() + " ORDER BY n1");

		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	/**
	 * Test TEXT format on a file with many fields with deprecated ACCESSOR
	 * TextFileAccessor and deprecated RESOLVER TextResolver
	 * 
	 * @throws Exception
	 */
	@Test
	public void textManyFieldsDeprecatedAccessorAndResolver() throws Exception {

		String csvPath = hdfsWorkingFolder + "/text_data.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

		hdfs.writeTextFile(csvPath, dataTable.getData(), ",");

		exTable = new ReadableExternalTable("bigtext", new String[] {
				"s1 text",
				"s2 text",
				"s3 text",
				"d1 timestamp",
				"n1 int",
				"n2 int",
				"n3 int",
				"n4 int",
				"n5 int",
				"n6 int",
				"n7 int",
				"s11 text",
				"s12 text",
				"s13 text",
				"d11 timestamp",
				"n11 int",
				"n12 int",
				"n13 int",
				"n14 int",
				"n15 int",
				"n16 int",
				"n17 int" }, (hdfsWorkingFolder + "/text_data.csv"), "TEXT");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.TextFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.TextResolver");
		exTable.setDelimiter(",");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY n1");

		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	/**
	 * Test TEXT format on a file with many fields with deprecated ACCESSOR
	 * LineReaderAccessor
	 * 
	 * @throws Exception
	 */
	@Test
	public void textManyFieldsDeprecatedAccessor() throws Exception {

		String csvPath = hdfsWorkingFolder + "/text_data.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

		hdfs.writeTextFile(csvPath, dataTable.getData(), ",");

		exTable = new ReadableExternalTable("bigtext", new String[] {
				"s1 text",
				"s2 text",
				"s3 text",
				"d1 timestamp",
				"n1 int",
				"n2 int",
				"n3 int",
				"n4 int",
				"n5 int",
				"n6 int",
				"n7 int",
				"s11 text",
				"s12 text",
				"s13 text",
				"d11 timestamp",
				"n11 int",
				"n12 int",
				"n13 int",
				"n14 int",
				"n15 int",
				"n16 int",
				"n17 int" }, (hdfsWorkingFolder + "/text_data.csv"), "TEXT");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.LineReaderAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");
		exTable.setDelimiter(",");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY n1");

		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	/**
	 * 
	 * Test Writable data inside a SequenceFile (read only).
	 * 
	 * @throws Exception
	 */
	@Test
	public void sequenceFile() throws Exception {

		Table sudoDataTable = new Table("dataTable", null);

		Object[] data = FileFormatsUtils.prepareData(new CustomSequencePreparer(), 100, sudoDataTable);

		hdfs.writeSequnceFile(data, (hdfsWorkingFolder + "/my_writable_inside_sequence.tbl"));

		ReadableExternalTable exTable = new ReadableExternalTable("seqwr", new String[] {
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
				"short5 smallint" }, (hdfsWorkingFolder + "/my_writable_inside_sequence.tbl"), "custom");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.WritableResolver");
		exTable.setDataSchema("CustomWritable");
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");

		ComparisonUtils.compareTables(exTable, sudoDataTable, report);
	}

	/**
	 * Test Avro data inside a SequenceFile (read only).
	 * 
	 * @throws Exception
	 */
	@Test
	public void avroFilesInsideSequence() throws Exception {

		Table dataTable = new Table("dataTable", null);

		String schemaName = "regression/resources/regressPXFCustomAvro.avsc";

		Object[] data = FileFormatsUtils.prepareData(new CustomAvroInSequencePreparer(schemaName), 100, dataTable);

		hdfs.writeAvroInSequnceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

		ReadableExternalTable exTable = new ReadableExternalTable("seqav", new String[] {
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
				"bl boolean" }, (hdfsWorkingFolder + "/avro_in_seq.tbl"), "custom");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.AvroResolver");
		exTable.setDataSchema("regressPXFCustomAvro.avsc");
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	/**
	 * Test Avro schema file (avsc) name with spaces
	 * 
	 * @throws Exception
	 */
	@Test
	public void avroFileNameWithSpaces() throws Exception {

		Table dataTable = new Table("dataTable", null);

		String schemaName = "regression/resources/regressPXFCustomAvro.avsc";

		Object[] data = FileFormatsUtils.prepareData(new CustomAvroInSequencePreparer(schemaName), 1000, dataTable);

		hdfs.writeAvroInSequnceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

		ReadableExternalTable exTable = new ReadableExternalTable("seqav_space", new String[] {
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
				"bl boolean" }, (hdfsWorkingFolder + "/avro_in_seq.tbl"), "custom");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.AvroResolver");
		exTable.setDataSchema("regress PXF Custom Avro1.avsc");
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	/**
	 * Test options are case insensitive
	 * 
	 * @throws Exception
	 */
	@Test
	public void optionsCaseInsensitive() throws Exception {

		Table dataTable = new Table("dataTable", null);

		String schemaName = "regression/resources/regressPXFCustomAvro.avsc";

		Object[] data = FileFormatsUtils.prepareData(new CustomAvroInSequencePreparer(schemaName), 1000, dataTable);

		hdfs.writeAvroInSequnceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

		ReadableExternalTable exTable = new ReadableExternalTable("seqav_case", new String[] {
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
				"bl boolean" }, (hdfsWorkingFolder + "/avro_in_seq.tbl"), "custom");

		exTable.setUserParameters(new String[] {
				"fragmenter=com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter",
				"Accessor=com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor",
				"ReSoLvEr=com.pivotal.pxf.plugins.hdfs.AvroResolver",
				"Data-Schema=regressPXFCustomAvro.avsc" });

		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	/**
	 * Test Avro data inside an AvroFile (read only).
	 * 
	 * @throws Exception
	 */
	@Test
	public void avroFilesInsideAvro() throws Exception {

		Table dataTable = new Table("dataTable", null);

		String avroSchemName = "regression/resources/regressPXFCustomAvro.avsc";

		IAvroSchema[] avroData = (IAvroSchema[]) FileFormatsUtils.prepareData(new CustomAvroPreparer(avroSchemName), 100, dataTable);

		hdfs.writeAvroFile((hdfsWorkingFolder + "/avro_in_avro.avro"), avroSchemName, avroData);

		ReadableExternalTable exTable = new ReadableExternalTable("avfav", new String[] {
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
				"bl boolean" }, (hdfsWorkingFolder + "/avro_in_avro.avro"), "custom");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.AvroFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.AvroResolver");
		exTable.setDataSchema("regressPXFCustomAvro.avsc");
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	// /**
	// * Test Protocol Buffer (read only). Create external table for the
	// * protocol-buffers and query the data
	// *
	// * @throws Exception
	// */
	// @Test
	// public void protocolBuffer() throws Exception {
	//
	// String protoBuffFile = (hdfsWorkingFolder + "/protobuf_data.tbl");
	//
	// Table dataTable = new Table("dataTable", null);
	//
	// Object[] generatedMessages = FileFormatsUtils.prepareData(new
	// CustomProtobuffPreparer(), 5, dataTable);
	//
	// hdfs.writeProtocolBufferFile(protoBuffFile, (GeneratedMessage)
	// generatedMessages[0]);
	//
	// ReadableExternalTable exTable = new ReadableExternalTable("pb", new
	// String[] {
	// "s1 text",
	// "num1 int",
	// "s2 text",
	// "s3 text",
	// "num2 int" }, protoBuffFile, "custom");
	//
	// exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
	// exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.ProtobufFileAccessor");
	// exTable.setResolver("com.pivotal.pxf.plugins.hdfs.ProtobufResolver");
	// exTable.setDataSchema("pbgp.desc");
	// exTable.setFormatter("pxfwritable_import");
	//
	// hawq.createTableAndVerify(exTable);
	//
	// hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() +
	// " ORDER BY num1;");
	//
	// ComparisonUtils.compareTables(exTable, dataTable, report);
	// }

	/**
	 * Test Quoted Line Break
	 * 
	 * @throws Exception
	 */
	@Test
	public void quotedLineBreak() throws Exception {

		String csvPath = hdfsWorkingFolder + "/small.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new QuotedLineTextPreparer(), 1000, dataTable);

		hdfs.writeTextFile(csvPath, dataTable.getData(), ",");

		exTable = new ReadableExternalTable("small_csv", new String[] {
				"num1 int",
				"word text",
				"num2 int" }, csvPath, "CSV");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.QuotedLineBreakAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");

		ComparisonUtils.compareTables(exTable, dataTable, report, "\"");
	}

	/**
	 * 
	 * Test wildcards in file name
	 * 
	 * @throws Exception
	 */
	@Test
	public void sequenceWildcardsAnyTbl() throws Exception {

		Table sudoDataTable = new Table("dataTable", null);

		Object[] data = FileFormatsUtils.prepareData(new CustomSequencePreparer(), 100, sudoDataTable);

		hdfs.writeSequnceFile(data, (hdfsWorkingFolder + "/wildcard/my_writable_inside_sequence.tbl"));

		ReadableExternalTable exTable = new ReadableExternalTable("seqwild", new String[] {
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
				"short5 smallint" }, (hdfsWorkingFolder + "/wildcard/*.tbl"), "custom");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.WritableResolver");
		exTable.setDataSchema("CustomWritable");
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");

		ComparisonUtils.compareTables(exTable, sudoDataTable, report);
	}

	/**
	 * 
	 * Test wildcards in file name
	 * 
	 * @throws Exception
	 */
	@Test
	public void sequenceWildcardsTabInName() throws Exception {

		Table sudoDataTable = new Table("dataTable", null);

		Object[] data = FileFormatsUtils.prepareData(new CustomSequencePreparer(), 100, sudoDataTable);

		sudoDataTable.pumpUpTableData(2);

		hdfs.writeSequnceFile(data, (hdfsWorkingFolder + "/wild/my_writable_inside_sequence1.tbl"));

		hdfs.writeSequnceFile(data, (hdfsWorkingFolder + "/wild/my_writable_inside_sequence2.tbl"));

		ReadableExternalTable exTable = new ReadableExternalTable("seqwild", new String[] {
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
				"short5 smallint" }, (hdfsWorkingFolder + "/wild/my_writable_inside_sequence?.tbl"), "custom");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.WritableResolver");
		exTable.setDataSchema("CustomWritable");
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");

		ComparisonUtils.compareTables(exTable, sudoDataTable, report);
	}

	/**
	 * 
	 * set bad host name
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeErrorInHostName() throws Exception {

		ReadableExternalTable exTable = new ReadableExternalTable("host_err", new String[] {
				"tmp1  timestamp",
				"num1  integer", }, (hdfsWorkingFolder + "/multiblock.tbl"), "TEXT");

		exTable.setHostname("badhostname");
		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.LineBreakAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");
		exTable.setDelimiter(",");

		hawq.createTableAndVerify(exTable);

		try {
			hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("Couldn't resolve host '" + exTable.getHostname() + "'", null), true);
		}
	}

	/**
	 * 
	 * set bad port
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeErrorInPort() throws Exception {

		ReadableExternalTable exTable = new ReadableExternalTable("port_err", new String[] {
				"tmp1  timestamp",
				"num1  integer", }, (hdfsWorkingFolder + "/multiblock.tbl"), "TEXT");

		exTable.setPort("12345");
		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.LineBreakAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");
		exTable.setDelimiter(",");

		hawq.createTableAndVerify(exTable);

		try {
			hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");
		} catch (Exception e) {

			/**
			 * Different Curel versions can provide different ERROR messages.
			 */
			String[] possibleErrMessages = {
					"Failed connect to " + exTable.getHostname() + ":" + exTable.getPort() + "; Connection refused",
					"couldn't connect to host" };

			ExceptionUtils.validate(report, e, new PSQLException(possibleErrMessages[0] + "|" + possibleErrMessages[1], null), true);
		}
	}

	public static void main(String[] args) {
		boolean b = RegexUtils.match("[couldn't connect to host | Failed connect to]", "Failed connect to");

		System.out.println(b);
	}

	/**
	 * 
	 * Test Empty File
	 * 
	 * @throws Exception
	 */
	@Test
	public void emptyFile() throws Exception {

		File regResourcesFolder = new File("regression/resources/");

		hdfs.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/empty.tbl", (hdfsWorkingFolder + "/empty.tbl"));

		ReadableExternalTable exTable = new ReadableExternalTable("empty", new String[] {
				"t1  text",
				"a1  integer" }, (hdfsWorkingFolder + "/empty.tbl"), "custom");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.SequenceFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.WritableResolver");
		exTable.setDataSchema("CustomWritable");
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY t1");

		ComparisonUtils.compareTables(exTable, new Table("emptySudoTable", null), report);
	}

	/**
	 * Test Analyze for HDFS File (read only).
	 * 
	 * @throws Exception
	 */
	@Test
	public void analyzeHdfsFile() throws Exception {

		File regResourcesFolder = new File("regression/resources/");

		hdfs.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/avroformat_inside_avrofile.avro", (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"));

		ReadableExternalTable exTable = new ReadableExternalTable("avfav_analyze_good", new String[] {
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
				"bt    bytea" }, (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"), "custom");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.AvroFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.AvroResolver");
		exTable.setDataSchema("regressPXFCustomAvro.avsc");
		exTable.setFormatter("pxfwritable_import");
		exTable.setAnalyzer("com.pivotal.pxf.plugins.hdfs.HdfsAnalyzer");

		hawq.createTableAndVerify(exTable);

		verifyAnalyze(exTable);

		ReportUtils.report(report, getClass(), "verify ANALYZE on path with directory");
		Assert.assertFalse(hdfs.list(hdfsWorkingFolder + "/").isEmpty());

		exTable.setName("analyze_dir");
		exTable.setPath(hdfsWorkingFolder);

		hawq.createTableAndVerify(exTable);
		verifyAnalyze(exTable);
	}

	/**
	 * Test Analyze for HDFS File (read only).
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeAnalyzeHdfsFileBadPort() throws Exception {

		File regResourcesFolder = new File("regression/resources/");

		hdfs.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/avroformat_inside_avrofile.avro", (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"));

		ReadableExternalTable exTable = new ReadableExternalTable("avfav_analyze_bad_port", new String[] {
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
				"bt    bytea" }, (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"), "custom");

		exTable.setPort("12345");
		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.AvroFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.AvroResolver");
		exTable.setDataSchema("regressPXFCustomAvro.avsc");
		exTable.setFormatter("pxfwritable_import");
		exTable.setAnalyzer("com.pivotal.pxf.plugins.hdfs.HdfsAnalyzer");

		hawq.createTableAndVerify(exTable);

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC on");

		hawq.runQuery("SET pxf_enable_stat_collection = true;");

		/**
		 * Different Curl versions can provide different ERROR messages.
		 */
		String[] possibleErrMessages = {
				"Failed connect to " + exTable.getHostname() + ":" + exTable.getPort() + "; Connection refused",
				"couldn't connect to host" };

		hawq.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(), possibleErrMessages[0] + "|" + possibleErrMessages[1], true);

		Table analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "	SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages = 1000 AND reltuples = 1000000");

		Table sudoResults = new Table("sudoResults", null);

		sudoResults.addRow(new String[] { "1" });

		ComparisonUtils.compareTables(analyzeResults, sudoResults, report);

		ReportUtils.stopLevel(report);
	}

	/**
	 * Test Analyze for HDFS File (read only).
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeAnalyzeHdfsFileBadClass() throws Exception {

		File regResourcesFolder = new File("regression/resources/");

		hdfs.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/avroformat_inside_avrofile.avro", (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"));

		ReadableExternalTable exTable = new ReadableExternalTable("avfav_analyze_bad_class", new String[] {
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
				"bt    bytea" }, (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"), "custom");

		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.AvroFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.AvroResolver");
		exTable.setDataSchema("regressPXFCustomAvro.avsc");
		exTable.setFormatter("pxfwritable_import");
		exTable.setAnalyzer("NoSuchAnalyzer");

		hawq.createTableAndVerify(exTable);

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC on");

		hawq.runQuery("SET pxf_enable_stat_collection = true;");

		String expectedWarning = "java.lang.ClassNotFoundException: NoSuchAnalyzer";

		hawq.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(), expectedWarning, true);

		Table analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "	SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages = 1000 AND reltuples = 1000000");

		Table sudoResults = new Table("sudoResults", null);

		sudoResults.addRow(new String[] { "1" });

		ComparisonUtils.compareTables(analyzeResults, sudoResults, report);

		ReportUtils.stopLevel(report);
	}

	/**
	 * Verify that filter pushdown is working, and we send a filter to PXF In
	 * this test we check that a query condition (WHERE ...) is serialized and
	 * passe correctly to PXF, by reading the debug logs of HAWQ.
	 * 
	 * The filter serialization is done using RPN, see more details in
	 * {@link com.pivotal.pxf.filtering.FilterParser} header.
	 * 
	 * @throws Exception
	 */
	@Test
	public void filterPushdown() throws Exception {

		String[] fields = new String[] { "s1 text", "n1 int", };
		String csvPath = hdfsWorkingFolder + "/text_data_small.csv";

		Table dataTable = new Table("dataTable", null);

		dataTable.addRow(new String[] { "nobody", "5" });
		dataTable.addRow(new String[] { "loves", "3" });
		dataTable.addRow(new String[] { "you", "10" });
		dataTable.addRow(new String[] { "when", "6" });
		dataTable.addRow(new String[] { "you're", "11" });
		dataTable.addRow(new String[] { "down", "4" });
		dataTable.addRow(new String[] { "and", "1" });
		dataTable.addRow(new String[] { "out", "0" });

		hdfs.writeTextFile(csvPath, dataTable.getData(), ",");

		exTable = TableFactory.getPxfReadableTextTable("filter_pushdown", fields, csvPath, ",");

		hawq.createTableAndVerify(exTable);

		ShellSystemObject sso = hawq.openPsql();
		try {

			// TODO: SET optimizer = false should be removed once GPSQL-1465 is
			// resolved.
			ReportUtils.report(report, getClass(), "set optimizer to false until ORCA fix pushdown to external scan");
			hawq.runSqlCmd(sso, "SET optimizer = false;", true);
			hawq.runSqlCmd(sso, "SET client_min_messages = debug2;", true);

			ReportUtils.report(report, getClass(), "filter with one condition");
			String queryFilter = "WHERE s1 = 'you'";
			String serializedFilter = "a0c\\\"you\\\"o5";
			verifyFilterPushdown(sso, queryFilter, serializedFilter, 1);

			ReportUtils.report(report, getClass(), "filter with AND condition");
			queryFilter = "WHERE s1 != 'nobody' AND n1 <= 5";
			serializedFilter = "a0c\\\"nobody\\\"o6a1c5o3o7";
			verifyFilterPushdown(sso, queryFilter, serializedFilter, 4);

			ReportUtils.report(report, getClass(), "no pushdown: filter with OR condition");
			queryFilter = "WHERE s1 = 'nobody' OR n1 <= 5";
			serializedFilter = null;
			verifyFilterPushdown(sso, queryFilter, serializedFilter, 5);

			ReportUtils.report(report, getClass(), "no pushdown: run no filter");
			queryFilter = "";
			serializedFilter = null;
			verifyFilterPushdown(sso, queryFilter, serializedFilter, 8);

			ReportUtils.report(report, getClass(), "set optimizer to true again");
			hawq.runSqlCmd(sso, "SET optimizer = true;", true);

			hawq.runSqlCmd(sso, "set client_min_messages = notice;", true);
		} finally {
			hawq.closePsql(sso);
		}
	}
	
	/**
	 * Test mapreduce.input.fileinputformat.input.dir.recursive parameter
	 * is working in PXF when set to true.
	 * The test runs a query on a table with directory name in its location, 
	 * and should successfully find all files in the nested directories.
	 * 
	 * @throws Exception
	 */
	@Test
	public void recursiveDirs() throws Exception {
		
		final String recursiveParam = "mapreduce.input.fileinputformat.input.dir.recursive";
		String baseDir = hdfsWorkingFolder + "/recursive";
		String[] fields = new String[] {
				"lyrics text",
				"line text",
		};
		String delim = ":";
		
		ReportUtils.report(report, getClass(), recursiveParam + " is TRUE, query should be successful");
		
		// build environment and prepare data:
		Table dataTable = prepareRecursiveDirsData(baseDir, delim);
	
		exTable = TableFactory.getPxfReadableTextTable("recursive_dirs", fields, baseDir, delim);
		hawq.createTableAndVerify(exTable);
		String sqlQuery = "SELECT * FROM " + exTable.getName() + " ORDER BY line";
		hawq.queryResults(exTable, sqlQuery);
		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	private Table prepareRecursiveDirsData(String baseDir, String delim) throws Exception {
		
		Table fileTable = new Table("file", null);
		Table dataTable = new Table("dataTable", null);
		
		/*
		 * - recursive/
		 *  	 -level1leaf1/
		 * 					 file1
		 *  	 -level1leaf2/
		 *   				 -level2leaf1/
		 *  							  file2
		 * 								  file3
		 *   				  	 		 -level3/
		 * 						    	   		 file4
		 *   				 -level2leaf2/
		 */
		hdfs.createDirectory(baseDir);
		hdfs.createDirectory(baseDir + "/level1leaf1");
		hdfs.createDirectory(baseDir + "/level1leaf2");
		hdfs.createDirectory(baseDir + "/level1leaf2/level2leaf1");
		hdfs.createDirectory(baseDir + "/level1leaf2/level2leaf1/level3");
		hdfs.createDirectory(baseDir + "/level1leaf2/level2leaf2");
		
		
		fileTable.addRow(new String[] {"When the truth is found to be lies", "line1" });
		fileTable.addRow(new String[] {"And all the joy within you dies", "line2" });
		hdfs.writeTextFile(baseDir + "/level1leaf1/file1", fileTable.getData(), delim);
		dataTable.appendRows(fileTable);
		fileTable.setData(null);

		fileTable.addRow(new String[] {"Don't you want somebody to love, don't you", "line3" });
		fileTable.addRow(new String[] {"Need somebody to love, wouldn't you", "line4" });
		hdfs.writeTextFile(baseDir + "/level1leaf2/level2leaf1/file2", fileTable.getData(), delim);
		dataTable.appendRows(fileTable);
		fileTable.setData(null);
		
		fileTable.addRow(new String[] {"Love somebody to love, you better", "line5" });
		hdfs.writeTextFile(baseDir + "/level1leaf2/level2leaf1/file3", fileTable.getData(), delim);
		dataTable.appendRows(fileTable);
		fileTable.setData(null);
		
		fileTable.addRow(new String[] {"Find somebody to love", "line6" });
		hdfs.writeTextFile(baseDir + "/level1leaf2/level2leaf1/level3/file4", fileTable.getData(), delim);
		dataTable.appendRows(fileTable);
		fileTable.setData(null);
		
		return dataTable;
	}
	
	private void verifyFilterPushdown(ShellSystemObject sso, String queryFilter, String serializedFilter, int rows)
			throws Exception {

		ReportUtils.startLevel(report, getClass(), "run query with filter '" + queryFilter + "'");
		String result = hawq.runSqlCmd(sso, "SELECT s1 FROM " + exTable.getName() + " " + queryFilter + ";", true);

		ReportUtils.report(report, getClass(), "verify query returned " + rows + " rows");
		if (rows == 1) {
			Assert.assertTrue("expecting 1 row", result.contains("1 row"));
		} else {
			Assert.assertTrue("expecting " + rows + " row", result.contains(rows + " row"));
		}
		if (serializedFilter == null) {
			ReportUtils.report(report, getClass(), "verify filter no was pushed");
			Assert.assertTrue("expecting to have no filter", result.contains("X-GP-HAS-FILTER: 0"));
		} else {
			ReportUtils.report(report, getClass(), "verify filter was pushed");
			Assert.assertTrue("expecting to have filter", result.contains("X-GP-HAS-FILTER: 1"));
			ReportUtils.report(report, getClass(), "verify filter is '" + serializedFilter + "'");
			Assert.assertTrue("expecting filter to be '" + serializedFilter + "'", result.contains("X-GP-FILTER: " + serializedFilter));
		}
		ReportUtils.stopLevel(report);
	}

	private void verifyAnalyze(ReadableExternalTable exTable) throws Exception {

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC off");

		hawq.runQuery("SET pxf_enable_stat_collection = false");

		hawq.analyze(exTable, true);

		Table analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages = 1000 AND reltuples = 1000000");

		Table sudoResults = new Table("sudoResults", null);

		sudoResults.addRow(new String[] { "1" });

		ComparisonUtils.compareTables(analyzeResults, sudoResults, report);

		ReportUtils.stopLevel(report);

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC on");

		hawq.runQuery("SET pxf_enable_stat_collection = true;");
		hawq.analyze(exTable);

		analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages != 1000 AND relpages > 0 AND reltuples != 1000000 AND reltuples > 0");

		ComparisonUtils.compareTables(analyzeResults, sudoResults, report);

		ReportUtils.stopLevel(report);

		ReportUtils.startLevel(report, getClass(), "verify that stats stay updated to most recent value after ANALYZE with GUC off");

		hawq.runQuery("SET pxf_enable_stat_collection = false;");
		hawq.analyze(exTable, true);

		analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages != 1000 AND relpages > 0 AND reltuples != 1000000 AND reltuples > 0");

		ComparisonUtils.compareTables(analyzeResults, sudoResults, report);

		ReportUtils.stopLevel(report);
	}
}
