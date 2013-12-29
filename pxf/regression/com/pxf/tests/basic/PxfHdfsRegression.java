package com.pxf.tests.basic;

import java.io.File;
import java.util.List;

import org.junit.Test;
import org.postgresql.util.PSQLException;

import com.google.protobuf.GeneratedMessage;
import com.pivotal.pxfauto.infra.fileformats.IAvroSchema;
import com.pivotal.pxfauto.infra.structures.tables.basic.Table;
import com.pivotal.pxfauto.infra.structures.tables.pxf.ReadableExternalTable;
import com.pivotal.pxfauto.infra.utils.exception.ExceptionUtils;
import com.pivotal.pxfauto.infra.utils.fileformats.FileFormatsUtils;
import com.pivotal.pxfauto.infra.utils.jsystem.report.ReportUtils;
import com.pivotal.pxfauto.infra.utils.tables.ComparisonUtils;
import com.pxf.tests.dataprepares.avro.CustomAvroPreparer;
import com.pxf.tests.dataprepares.protobuf.schema.CustomProtobuffPreparer;
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
		exTable = new ReadableExternalTable("gphdfs_in", new String[] { "a int", "b text", "c bytea" }, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

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
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("AvroResolver");
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
	 * Use "HdfsDataFragmenter" + "LineReaderAccessor" + "StringPassResolver"
	 * and TEXT delimiter for parsing CSV file.
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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("LineBreakAccessor");
		exTable.setResolver("StringPassResolver");
		exTable.setDelimiter(",");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM bigtext ORDER BY n1");

		ComparisonUtils.compareTables(exTable, dataTable, report);
		
		ReportUtils.report(report, getClass(), "single condition - remove elements from dataTable and compare");
		hawq.queryResults(exTable, "SELECT * FROM bigtext " +
				"WHERE n2 > 500 ORDER BY n1");
		
		List<List<String>> data = dataTable.getData();
		dataTable.setData(data.subList(50, 1000));
		
		ComparisonUtils.compareTables(exTable, dataTable, report);
		
		ReportUtils.report(report, getClass(), "two conditions - remove elements from dataTable and compare");
		hawq.queryResults(exTable, "SELECT * FROM bigtext " +
				"WHERE (n2 > 500) AND (n1 <= 60) ORDER BY n1");
		
		dataTable.setData(data.subList(50, 60));
		
		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	/**
	 * Use "HdfsDataFragmenter" + "LineReaderAccessor" + "StringPassResolver"
	 * along with Format("CSV")
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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("LineBreakAccessor");
		exTable.setResolver("StringPassResolver");

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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("TextFileAccessor");
		exTable.setResolver("TextResolver");
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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("LineReaderAccessor");
		exTable.setResolver("StringPassResolver");
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

		hdfs
				.writeSequnceFile(data, (hdfsWorkingFolder + "/my_writable_inside_sequence.tbl"));

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
				"short5 smallint"
		}, (hdfsWorkingFolder + "/my_writable_inside_sequence.tbl"), "custom");

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("WritableResolver");
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

		hdfs
				.writeAvroInSequnceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("AvroResolver");
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

		hdfs
				.writeAvroInSequnceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("AvroResolver");
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

		hdfs
				.writeAvroInSequnceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

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
				"fragmenter=HdfsDataFragmenter",
				"Accessor=SequenceFileAccessor",
				"ReSoLvEr=AvroResolver",
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

		hdfs
				.writeAvroFile((hdfsWorkingFolder + "/avro_in_avro.avro"), avroSchemName, avroData);

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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("AvroFileAccessor");
		exTable.setResolver("AvroResolver");
		exTable.setDataSchema("regressPXFCustomAvro.avsc");
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

	/**
	 * Test Protocol Buffer (read only). Create external table for the
	 * protocol-buffers and query the data
	 * 
	 * @throws Exception
	 */
	@Test
	public void protocolBuffer() throws Exception {

		String protoBuffFile = (hdfsWorkingFolder + "/protobuf_data.tbl");

		Table dataTable = new Table("dataTable", null);

		Object[] generatedMessages = FileFormatsUtils.prepareData(new CustomProtobuffPreparer(), 5, dataTable);

		hdfs
				.writeProtocolBufferFile(protoBuffFile, (GeneratedMessage) generatedMessages[0]);

		ReadableExternalTable exTable = new ReadableExternalTable("pb", new String[] {
				"s1 text",
				"num1 int",
				"s2 text",
				"s3 text",
				"num2 int" }, protoBuffFile, "custom");

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("ProtobufFileAccessor");
		exTable.setResolver("ProtobufResolver");
		exTable.setDataSchema("pbgp.desc");
		exTable.setFormatter("pxfwritable_import");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		ComparisonUtils.compareTables(exTable, dataTable, report);
	}

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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("QuotedLineBreakAccessor");
		exTable.setResolver("StringPassResolver");

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

		hdfs
				.writeSequnceFile(data, (hdfsWorkingFolder + "/wildcard/my_writable_inside_sequence.tbl"));

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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("WritableResolver");
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

		hdfs
				.writeSequnceFile(data, (hdfsWorkingFolder + "/wild/my_writable_inside_sequence2.tbl"));

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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("WritableResolver");
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
		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("LineBreakAccessor");
		exTable.setResolver("StringPassResolver");
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
		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("LineBreakAccessor");
		exTable.setResolver("StringPassResolver");
		exTable.setDelimiter(",");

		hawq.createTableAndVerify(exTable);

		try {
			hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("Failed connect to " + exTable.getHostname() + ":" + exTable.getPort() + "; Connection refused", null), true);
		}
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

		hdfs
				.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/empty.tbl", (hdfsWorkingFolder + "/empty.tbl"));

		ReadableExternalTable exTable = new ReadableExternalTable("empty", new String[] {
				"t1  text",
				"a1  integer" }, (hdfsWorkingFolder + "/empty.tbl"), "custom");

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("WritableResolver");
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

		hdfs
				.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/avroformat_inside_avrofile.avro", (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"));

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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("AvroFileAccessor");
		exTable.setResolver("AvroResolver");
		exTable.setDataSchema("regressPXFCustomAvro.avsc");
		exTable.setFormatter("pxfwritable_import");
		exTable.setAnalyzer("HdfsAnalyzer");

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

		hdfs
				.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/avroformat_inside_avrofile.avro", (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"));

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
		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("AvroFileAccessor");
		exTable.setResolver("AvroResolver");
		exTable.setDataSchema("regressPXFCustomAvro.avsc");
		exTable.setFormatter("pxfwritable_import");
		exTable.setAnalyzer("HdfsAnalyzer");

		hawq.createTableAndVerify(exTable);

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC on");

		hawq.runQuery("SET pxf_enable_stat_collection = true;");

		String expectedWarning = "skipping \"" + exTable.getName() + "\" --- error returned: remote component error (0): Failed connect to localhost:" + exTable.getPort() + "; Connection refused";

		hawq.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(), expectedWarning, false);

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

		hdfs
				.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/avroformat_inside_avrofile.avro", (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"));

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

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("AvroFileAccessor");
		exTable.setResolver("AvroResolver");
		exTable.setDataSchema("regressPXFCustomAvro.avsc");
		exTable.setFormatter("pxfwritable_import");
		exTable.setAnalyzer("NoSuchAnalyzer");

		hawq.createTableAndVerify(exTable);

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC on");

		hawq.runQuery("SET pxf_enable_stat_collection = true;");

		String expectedWarning = "Class NoSuchAnalyzer could not be found on the CLASSPATH. NoSuchAnalyzer";

		hawq.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(), expectedWarning, true);

		Table analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "	SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages = 1000 AND reltuples = 1000000");

		Table sudoResults = new Table("sudoResults", null);

		sudoResults.addRow(new String[] { "1" });

		ComparisonUtils.compareTables(analyzeResults, sudoResults, report);

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
