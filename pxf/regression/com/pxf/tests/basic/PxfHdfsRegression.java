package com.pxf.tests.basic;

import java.io.File;

import org.junit.Test;
import org.postgresql.util.PSQLException;

import com.pxf.infra.structures.tables.basic.Table;
import com.pxf.infra.structures.tables.pxf.ReadbleExternalTable;
import com.pxf.infra.utils.exception.ExceptionUtils;
import com.pxf.infra.utils.fileformats.FileFormatsUtils;
import com.pxf.infra.utils.fileformats.avro.schema.CustomrAvroInSequenceReader;
import com.pxf.infra.utils.fileformats.avro.schema.CustomrAvroReader;
import com.pxf.infra.utils.fileformats.avro.schema.IAvroSchema;
import com.pxf.infra.utils.fileformats.sequence.CustomSequenceReader;
import com.pxf.infra.utils.jsystem.report.ReportUtils;
import com.pxf.tests.testcases.PxfTestCase;

/**
 * PXF on HDFS Regression tests
 */
public class PxfHdfsRegression extends PxfTestCase {

	ReadbleExternalTable exTable;

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
		exTable = new ReadbleExternalTable("gphdfs_in", new String[] {
				"a int",
				"b text",
				"c bytea" }, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

		exTable.setFragmenter("xfrag");
		exTable.setAccessor("xacc");
		exTable.setResolver("xres");
		exTable.setUserParamters(new String[] { "someuseropt=someuserval" });
		exTable.setFormatter("pxfwritable_import");

		createHawqTable(exTable);

		/**
		 * gphdfs_in1
		 */
		exTable.setName("gphdfs_in1");
		exTable.setPath(hdfsWorkingFolder);
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("AvroResolver");
		exTable.setDataSchema("MySchema");

		exTable.setUserParamters(null);

		createHawqTable(exTable);
	}

	/**
	 * Create Table with no Fragmenter, Accessor and Resolver. Should fail and
	 * throw the right message.
	 * 
	 * @throws Exception
	 */
	@Test
	public void neagtiveNoFragmenterNoAccossorNoResolver() throws Exception {

		exTable = new ReadbleExternalTable("gphdfs_in2", new String[] {
				"a int",
				"b text",
				"c bytea" }, (hdfsWorkingFolder + "/*"), "CUSTOM");

		exTable.setFormatter("pxfwritable_import");

		try {

			createHawqTable(exTable);

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

		exTable = new ReadbleExternalTable("gphdfs_in2", new String[] {
				"a int",
				"b text",
				"c bytea" }, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

		exTable.setAccessor("xacc");
		exTable.setResolver("xres");
		exTable.setUserParamters(new String[] { "someuseropt=someuserval" });
		exTable.setFormatter("pxfwritable_import");

		try {
			createHawqTable(exTable);
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

		exTable = new ReadbleExternalTable("gphdfs_in2", new String[] {
				"a int",
				"b text",
				"c bytea" }, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

		exTable.setFragmenter("xfrag");
		exTable.setResolver("xres");
		exTable.setUserParamters(new String[] { "someuseropt=someuserval" });
		exTable.setFormatter("pxfwritable_import");

		try {
			createHawqTable(exTable);
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

		exTable = new ReadbleExternalTable("gphdfs_in2", new String[] {
				"a int",
				"b text",
				"c bytea" }, ("somepath/" + hdfsWorkingFolder), "CUSTOM");

		exTable.setFragmenter("xfrag");
		exTable.setAccessor("xacc");
		exTable.setFormatter("pxfwritable_import");

		try {

			createHawqTable(exTable);

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

		File localCsvFile = new File("regression/resources/text_data.csv");

		hdfs.getFunc()
				.copyFromLocal(localCsvFile.getAbsolutePath(), (hdfsWorkingFolder + "/text_data.csv"));

		exTable = new ReadbleExternalTable("bigtext", new String[] {
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
		exTable.setAccessor("LineBreakAccessor");
		exTable.setResolver("StringPassResolver");
		exTable.setDelimiter(",");

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM bigtext ORDER BY n1");

		compareCsv(localCsvFile, exTable);
	}

	/**
	 * Use "HdfsDataFragmenter" + "LineReaderAccessor" + "StringPassResolver"
	 * along with Format("CSV")
	 * 
	 * @throws Exception
	 */
	@Test
	public void textCsvFormatOnManyFields() throws Exception {

		File localCsvFile = new File("regression/resources/text_data.csv");

		hdfs.getFunc()
				.copyFromLocal(localCsvFile.getAbsolutePath(), (hdfsWorkingFolder + "/text_data.csv"));

		exTable = new ReadbleExternalTable("bigtext", new String[] {
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

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM  " + exTable.getName() + " ORDER BY n1");

		compareCsv(localCsvFile, exTable);
	}

	/**
	 * Test TEXT format on a file with many fields with deprecated ACCESSOR
	 * TextFileAccessor and deprecated RESOLVER TextResolver
	 * 
	 * @throws Exception
	 */
	@Test
	public void textManyFieldsFeprecatedAccessorAndResolver() throws Exception {

		File localCsvFile = new File("regression/resources/text_data.csv");

		hdfs.getFunc()
				.copyFromLocal(localCsvFile.getAbsolutePath(), (hdfsWorkingFolder + "/text_data.csv"));

		exTable = new ReadbleExternalTable("bigtext", new String[] {
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

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY n1");

		compareCsv(localCsvFile, exTable);
	}

	/**
	 * Test TEXT format on a file with many fields with deprecated ACCESSOR
	 * LineReaderAccessor
	 * 
	 * @throws Exception
	 */
	@Test
	public void textManyFieldsFeprecatedAccessor() throws Exception {

		File localCsvFile = new File("regression/resources/text_data.csv");

		hdfs.getFunc()
				.copyFromLocal(localCsvFile.getAbsolutePath(), (hdfsWorkingFolder + "/text_data.csv"));

		exTable = new ReadbleExternalTable("bigtext", new String[] {
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

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY n1");

		compareCsv(localCsvFile, exTable);
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

		Object[] data = FileFormatsUtils.prepareData(new CustomSequenceReader(), 100, sudoDataTable);

		hdfs.getFunc()
				.writeSequnceFile(data, (hdfsWorkingFolder + "/my_writable_inside_sequence.tbl"));

		ReadbleExternalTable exTable = new ReadbleExternalTable("seqwr", new String[] {
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
				"bool3 boolean" }, (hdfsWorkingFolder + "/my_writable_inside_sequence.tbl"), "custom");

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("WritableResolver");
		exTable.setDataSchema("CustomWritable");
		exTable.setFormatter("pxfwritable_import");

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");

		compareTables(exTable, sudoDataTable);
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

		Object[] data = FileFormatsUtils.prepareData(new CustomrAvroInSequenceReader(schemaName), 100, dataTable);

		hdfs.getFunc()
				.writeAvroInSequnceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

		ReadbleExternalTable exTable = new ReadbleExternalTable("seqav", new String[] {
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

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		compareTables(exTable, dataTable);
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

		Object[] data = FileFormatsUtils.prepareData(new CustomrAvroInSequenceReader(schemaName), 1000, dataTable);

		hdfs.getFunc()
				.writeAvroInSequnceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

		ReadbleExternalTable exTable = new ReadbleExternalTable("seqav_space", new String[] {
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

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		compareTables(exTable, dataTable);
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

		Object[] data = FileFormatsUtils.prepareData(new CustomrAvroInSequenceReader(schemaName), 1000, dataTable);

		hdfs.getFunc()
				.writeAvroInSequnceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

		ReadbleExternalTable exTable = new ReadbleExternalTable("seqav_case", new String[] {
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

		exTable.setUserParamters(new String[] {
				"fragmenter=HdfsDataFragmenter",
				"Accessor=SequenceFileAccessor",
				"ReSoLvEr=AvroResolver",
				"Data-Schema=regressPXFCustomAvro.avsc" });

		exTable.setFormatter("pxfwritable_import");

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		compareTables(exTable, dataTable);
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

		IAvroSchema[] avroData = (IAvroSchema[]) FileFormatsUtils.prepareData(new CustomrAvroReader(avroSchemName), 100, dataTable);

		hdfs.getFunc()
				.writeAvroFile((hdfsWorkingFolder + "/avro_in_avro.avro"), avroSchemName, avroData);

		ReadbleExternalTable exTable = new ReadbleExternalTable("avfav", new String[] {
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

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		compareTables(exTable, dataTable);
	}

	/**
	 * Test Protocol Buffer (read only). Create external table for the
	 * protocol-buffers and query the data
	 * 
	 * @throws Exception
	 */
	@Test
	public void protocolBuffer() throws Exception {

		File regResourcesFolder = new File("regression/resources/");

		hdfs.getFunc()
				.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/protobuf_data.tbl", (hdfsWorkingFolder + "/protobuf_data.tbl"));

		ReadbleExternalTable exTable = new ReadbleExternalTable("pb", new String[] {
				"s1 text",
				"num1 int",
				"s2 text",
				"s3 text",
				"num2 int" }, (hdfsWorkingFolder + "/protobuf_data.tbl"), "custom");

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("ProtobufFileAccessor");
		exTable.setResolver("ProtobufResolver");
		exTable.setDataSchema("pbgp.desc");
		exTable.setFormatter("pxfwritable_import");

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1;");

		// TODO: get protbuf into table and compare it
	}

	/**
	 * Test Quoted Line Break
	 * 
	 * @throws Exception
	 */
	@Test
	public void quotedLineBreak() throws Exception {

		File localCsvFile = new File("regression/resources/small.csv");

		hdfs.getFunc()
				.copyFromLocal(localCsvFile.getAbsolutePath(), (hdfsWorkingFolder + "/small.csv"));

		exTable = new ReadbleExternalTable("small_csv", new String[] {
				"num1 int",
				"word text",
				"num2 int" }, (hdfsWorkingFolder + "/small.csv"), "CSV");

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("QuotedLineBreakAccessor");
		exTable.setResolver("StringPassResolver");

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");

		compareCsv(localCsvFile, exTable);
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

		Object[] data = FileFormatsUtils.prepareData(new CustomSequenceReader(), 100, sudoDataTable);

		hdfs.getFunc()
				.writeSequnceFile(data, (hdfsWorkingFolder + "/wildcard/my_writable_inside_sequence.tbl"));

		ReadbleExternalTable exTable = new ReadbleExternalTable("seqwild", new String[] {
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
				"bool3 boolean" }, (hdfsWorkingFolder + "/wildcard/*.tbl"), "custom");

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("WritableResolver");
		exTable.setDataSchema("CustomWritable");
		exTable.setFormatter("pxfwritable_import");

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");

		compareTables(exTable, sudoDataTable);
	}

	/**
	 * 
	 * Test wildcards in file name
	 * 
	 * @throws Exception
	 */
	@Test
	public void sequenceWildcardsTabInName() throws Exception {

		File regResourcesFolder = new File("regression/resources/");

		hdfs.getFunc()
				.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/writable_inside_sequence1.tbl", (hdfsWorkingFolder + "/wild/writable_inside_sequence.tbl"));

		ReadbleExternalTable exTable = new ReadbleExternalTable("seqwild", new String[] {
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
				"bool3 boolean" }, (hdfsWorkingFolder + "/wild/writable_inside_sequence.tbl"), "custom");

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("WritableResolver");
		exTable.setDataSchema("CustomWritable");
		exTable.setFormatter("pxfwritable_import");

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");
	}

	/**
	 * 
	 * set bad host name
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeErrorInHostName() throws Exception {

		ReadbleExternalTable exTable = new ReadbleExternalTable("host_err", new String[] {
				"tmp1  timestamp",
				"num1  integer", }, (hdfsWorkingFolder + "/multiblock.tbl"), "TEXT");

		exTable.setHostname("badhostname");
		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("LineBreakAccessor");
		exTable.setResolver("StringPassResolver");
		exTable.setDelimiter(",");

		createHawqTable(exTable);

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

		ReadbleExternalTable exTable = new ReadbleExternalTable("port_err", new String[] {
				"tmp1  timestamp",
				"num1  integer", }, (hdfsWorkingFolder + "/multiblock.tbl"), "TEXT");

		exTable.setPort("12345");
		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("LineBreakAccessor");
		exTable.setResolver("StringPassResolver");
		exTable.setDelimiter(",");

		createHawqTable(exTable);

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

		hdfs.getFunc()
				.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/empty.tbl", (hdfsWorkingFolder + "/empty.tbl"));

		ReadbleExternalTable exTable = new ReadbleExternalTable("empty", new String[] {
				"t1  text",
				"a1  integer" }, (hdfsWorkingFolder + "/empty.tbl"), "custom");

		exTable.setFragmenter("HdfsDataFragmenter");
		exTable.setAccessor("SequenceFileAccessor");
		exTable.setResolver("WritableResolver");
		exTable.setDataSchema("CustomWritable");
		exTable.setFormatter("pxfwritable_import");

		createHawqTable(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY t1");

		compareTables(exTable, new Table("emptySudoTable", null));
	}

	/**
	 * Test Analyze for HDFS File (read only).
	 * 
	 * @throws Exception
	 */
	@Test
	public void analyzeHdfsFile() throws Exception {

		File regResourcesFolder = new File("regression/resources/");

		hdfs.getFunc()
				.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/avroformat_inside_avrofile.avro", (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"));

		ReadbleExternalTable exTable = new ReadbleExternalTable("avfav_analyze_good", new String[] {
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

		createHawqTable(exTable);

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

		hdfs.getFunc()
				.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/avroformat_inside_avrofile.avro", (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"));

		ReadbleExternalTable exTable = new ReadbleExternalTable("avfav_analyze_bad_port", new String[] {
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

		createHawqTable(exTable);

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC on");

		hawq.runQuery("SET pxf_enable_stat_collection = true;");

		String expectedWarning = "skipping \"" + exTable.getName() + "\" --- error returned: remote component error (0): Failed connect to localhost:" + exTable.getPort() + "; Connection refused";

		hawq.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(), expectedWarning, false);

		Table analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "	SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages = 1000 AND reltuples = 1000000");

		Table sudoResults = new Table("sudoResults", null);

		sudoResults.addRow(new String[] { "1" });

		compareTables(analyzeResults, sudoResults);

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

		hdfs.getFunc()
				.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/avroformat_inside_avrofile.avro", (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"));

		ReadbleExternalTable exTable = new ReadbleExternalTable("avfav_analyze_bad_class", new String[] {
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

		createHawqTable(exTable);

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC on");

		hawq.runQuery("SET pxf_enable_stat_collection = true;");

		String expectedWarning = "Class NoSuchAnalyzer could not be found on the CLASSPATH. NoSuchAnalyzer";

		hawq.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(), expectedWarning, true);

		Table analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "	SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages = 1000 AND reltuples = 1000000");

		Table sudoResults = new Table("sudoResults", null);

		sudoResults.addRow(new String[] { "1" });

		compareTables(analyzeResults, sudoResults);

		ReportUtils.stopLevel(report);
	}

	private void verifyAnalyze(ReadbleExternalTable exTable) throws Exception {

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC off");

		hawq.runQuery("SET pxf_enable_stat_collection = false");

		hawq.analyze(exTable, true);

		Table analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages = 1000 AND reltuples = 1000000");

		Table sudoResults = new Table("sudoResults", null);

		sudoResults.addRow(new String[] { "1" });

		compareTables(analyzeResults, sudoResults);

		ReportUtils.stopLevel(report);

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC on");

		hawq.runQuery("SET pxf_enable_stat_collection = true;");
		hawq.analyze(exTable);

		analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages != 1000 AND relpages > 0 AND reltuples != 1000000 AND reltuples > 0");

		compareTables(analyzeResults, sudoResults);

		ReportUtils.stopLevel(report);

		ReportUtils.startLevel(report, getClass(), "verify that stats stay updated to most recent value after ANALYZE with GUC off");

		hawq.runQuery("SET pxf_enable_stat_collection = false;");
		hawq.analyze(exTable, true);

		analyzeResults = new Table("results", null);

		hawq.queryResults(analyzeResults, "SELECT COUNT(*) FROM pg_class WHERE relname = '" + exTable.getName() + "' AND relpages != 1000 AND relpages > 0 AND reltuples != 1000000 AND reltuples > 0");

		compareTables(analyzeResults, sudoResults);

		ReportUtils.stopLevel(report);
	}
}
