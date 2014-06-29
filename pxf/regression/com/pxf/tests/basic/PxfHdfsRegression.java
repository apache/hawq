package com.pxf.tests.basic;

import java.io.File;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import com.pivotal.parot.fileformats.IAvroSchema;
import com.pivotal.parot.structures.tables.basic.Table;
import com.pivotal.parot.structures.tables.pxf.ErrorTable;
import com.pivotal.parot.structures.tables.pxf.ReadableExternalTable;
import com.pivotal.parot.structures.tables.utils.TableFactory;
import com.pivotal.parot.utils.exception.ExceptionUtils;
import com.pivotal.parot.utils.fileformats.FileFormatsUtils;
import com.pivotal.parot.utils.jsystem.report.ReportUtils;
import com.pivotal.parot.utils.regex.RegexUtils;
import com.pivotal.parot.utils.tables.ComparisonUtils;
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
	 * Create Table with no Fragmenter, Accessor and Resolver. Should fail and throw the right
	 * message.
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

		} catch (PSQLException e) {
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
		} catch (PSQLException e) {

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
		} catch (PSQLException e) {
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

		} catch (PSQLException e) {
			ExceptionUtils.validate(report, e, new PSQLException("ERROR: Invalid URI pxf://" + hawq.getHost() + ":50070/" + exTable.getPath() + "?FRAGMENTER=xfrag&ACCESSOR=xacc: PROFILE or RESOLVER option(s) missing", null), false);
		}
	}

	/**
	 * Use "com.pivotal.pxf.plugins.hdfs.fragmenters.HdfsDataFragmenter" + "LineReaderAccessor" +
	 * "StringPassResolver" and TEXT delimiter for parsing CSV file.
	 * 
	 * @throws Exception
	 */
	@Test
	public void textFormatsManyFields() throws Exception {

		String csvPath = hdfsWorkingFolder + "/text_data.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

		hdfs.writeTableToFile(csvPath, dataTable, ",");

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
	 * Use "com.pivotal.pxf.plugins.hdfs.fragmenters.HdfsDataFragmenter" + "LineReaderAccessor" +
	 * "StringPassResolver" along with Format("CSV")
	 * 
	 * @throws Exception
	 */
	@Test
	public void textCsvFormatOnManyFields() throws Exception {

		String csvPath = hdfsWorkingFolder + "/text_data.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

		hdfs.writeTableToFile(csvPath, dataTable, ",");

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
	 * Test TEXT format on a file with many fields with deprecated ACCESSOR TextFileAccessor and
	 * deprecated RESOLVER TextResolver
	 * 
	 * @throws Exception
	 */
	@Test
	public void textManyFieldsDeprecatedAccessorAndResolver() throws Exception {

		String csvPath = hdfsWorkingFolder + "/text_data.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

		hdfs.writeTableToFile(csvPath, dataTable, ",");

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
	 * Test TEXT format on a file with many fields with deprecated ACCESSOR LineReaderAccessor
	 * 
	 * @throws Exception
	 */
	@Test
	public void textManyFieldsDeprecatedAccessor() throws Exception {

		String csvPath = hdfsWorkingFolder + "/text_data.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

		hdfs.writeTableToFile(csvPath, dataTable, ",");

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

		hdfs.writeSequenceFile(data, (hdfsWorkingFolder + "/my_writable_inside_sequence.tbl"));

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
				"short5 smallint" }, hdfsWorkingFolder + "/my_writable_inside_sequence.tbl", "custom");

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

		String schemaName = "resources/regressPXFCustomAvro.avsc";

		Object[] data = FileFormatsUtils.prepareData(new CustomAvroInSequencePreparer(schemaName), 100, dataTable);

		hdfs.writeAvroInSequenceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

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

		String schemaName = "resources/regressPXFCustomAvro.avsc";

		Object[] data = FileFormatsUtils.prepareData(new CustomAvroInSequencePreparer(schemaName), 1000, dataTable);

		hdfs.writeAvroInSequenceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

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

		String schemaName = "resources/regressPXFCustomAvro.avsc";

		Object[] data = FileFormatsUtils.prepareData(new CustomAvroInSequencePreparer(schemaName), 1000, dataTable);

		hdfs.writeAvroInSequenceFile(hdfsWorkingFolder + "/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);

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

		String avroSchemName = "resources/regressPXFCustomAvro.avsc";

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

		hdfs.writeTableToFile(csvPath, dataTable, ",");

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

		hdfs.writeSequenceFile(data, (hdfsWorkingFolder + "/wildcard/my_writable_inside_sequence.tbl"));

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

		hdfs.writeSequenceFile(data, (hdfsWorkingFolder + "/wild/my_writable_inside_sequence1.tbl"));

		hdfs.writeSequenceFile(data, (hdfsWorkingFolder + "/wild/my_writable_inside_sequence2.tbl"));

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

		exTable.setHost("badhostname");
		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.LineBreakAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");
		exTable.setDelimiter(",");

		hawq.createTableAndVerify(exTable);

		try {
			hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num1");
		} catch (PSQLException e) {
			ExceptionUtils.validate(report, e, new PSQLException("Couldn't resolve host '" + exTable.getHost() + "'", null), true);
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

		File regResourcesFolder = new File("resources/");

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

		File regResourcesFolder = new File("resources/");

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
	public void negativeAnalyzeHdfsFileBadHost() throws Exception {

		File regResourcesFolder = new File("resources/");

		hdfs.copyFromLocal(regResourcesFolder.getAbsolutePath() + "/avroformat_inside_avrofile.avro", (hdfsWorkingFolder + "/avroformat_inside_avrofile.avro"));

		ReadableExternalTable exTable = new ReadableExternalTable("avfav_analyze_bad_host", new String[] {
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

		exTable.setHost("host_err");
		exTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		exTable.setAccessor("com.pivotal.pxf.plugins.hdfs.AvroFileAccessor");
		exTable.setResolver("com.pivotal.pxf.plugins.hdfs.AvroResolver");
		exTable.setFormatter("pxfwritable_import");
		exTable.setAnalyzer("com.pivotal.pxf.plugins.hdfs.HdfsAnalyzer");

		hawq.createTableAndVerify(exTable);

		ReportUtils.startLevel(report, getClass(), "verify that default stats remain after ANALYZE with GUC on");

		hawq.runQuery("SET pxf_enable_stat_collection = true;");

		hawq.runQueryWithExpectedWarning("ANALYZE " + exTable.getName(), "remote component error \\(0\\): Couldn't resolve host 'host_err'", true);

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

		File regResourcesFolder = new File("resources/");

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
	 * Verify that filter pushdown is working, and we send a filter to PXF. In this test we check
	 * that a query condition (WHERE ...) is serialized and passed correctly to PXF, 
	 * by using FilterPrinterAccessor that prints the received filter from HAWQ.
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

		hdfs.writeTableToFile(csvPath, dataTable, ",");
		
		ReadableExternalTable printFilterTable = 
				new ReadableExternalTable("filter_printer", fields, csvPath, "TEXT"); 
		printFilterTable.setFragmenter("com.pivotal.pxf.plugins.hdfs.HdfsDataFragmenter");
		printFilterTable.setAccessor("FilterPrinterAccessor");
		printFilterTable.setResolver("com.pivotal.pxf.plugins.hdfs.StringPassResolver");
		printFilterTable.setDelimiter(",");
		hawq.createTableAndVerify(printFilterTable);

		exTable = TableFactory.getPxfReadableTextTable("filter_pushdown", fields, csvPath, ",");
		hawq.createTableAndVerify(exTable);

		ReportUtils.report(report, getClass(), "set pxf_enable_filter_pushdown to true");
		hawq.runQuery("SET pxf_enable_filter_pushdown = true;");
		
		ReportUtils.report(report, getClass(), "filter with one condition");
		String queryFilter = "WHERE s1 = 'you'";
		String serializedFilter = "a0c\\\"you\\\"o5";
		verifyFilterPushdown(printFilterTable, queryFilter, serializedFilter, 1);

		ReportUtils.report(report, getClass(), "filter with AND condition");
		queryFilter = "WHERE s1 != 'nobody' AND n1 <= 5";
		serializedFilter = "a0c\\\"nobody\\\"o6a1c5o3o7";
		verifyFilterPushdown(printFilterTable, queryFilter, serializedFilter, 4);

		ReportUtils.report(report, getClass(), "no pushdown: filter with OR condition");
		queryFilter = "WHERE s1 = 'nobody' OR n1 <= 5";
		serializedFilter = null;
		verifyFilterPushdown(printFilterTable, queryFilter, serializedFilter, 5);

		ReportUtils.report(report, getClass(), "no pushdown: run no filter");
		queryFilter = "";
		serializedFilter = null;
		verifyFilterPushdown(printFilterTable, queryFilter, serializedFilter, 8);

		ReportUtils.report(report, getClass(), "set pxf_enable_filter_pushdown to false");
		hawq.runQuery("SET pxf_enable_filter_pushdown = false;");

		ReportUtils.report(report, getClass(), "filter with one condition - filter pushdown disabled");
		queryFilter = "WHERE s1 = 'you'";
		serializedFilter = null;
		verifyFilterPushdown(printFilterTable, queryFilter, serializedFilter, 1);

		ReportUtils.report(report, getClass(), "set pxf_enable_filter_pushdown to true again");
		hawq.runQuery("SET pxf_enable_filter_pushdown = true;");

	}

	/**
	 * Test mapreduce.input.fileinputformat.input.dir.recursive parameter is working in PXF when set
	 * to true. The test runs a query on a table with directory name in its location, and should
	 * successfully find all files in the nested directories.
	 * 
	 * @throws Exception
	 */
	@Test
	public void recursiveDirs() throws Exception {

		final String recursiveParam = "mapreduce.input.fileinputformat.input.dir.recursive";
		String baseDir = hdfsWorkingFolder + "/recursive";
		String[] fields = new String[] { "lyrics text", "line text", };
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

	/**
	 * Test error table option: LOG ERRORS INTO <err_table> SEGMENT REJECT LIMIT <number_of_errors>
	 * When reading data from external table, errors in formatting are stored in an error table
	 * until the limit of allowed errors is reached.
	 * 
	 * The test covers a case when the number of errors is lower than limit and a case when the
	 * errors breach the limit. It also tests the cleanup of segwork and metadata information in the
	 * filename parameter (the pxf URI) in the error table (GPSQL-1708).
	 * 
	 * @throws Exception
	 */
	@Test
	public void errorTables() throws Exception {

		String[] fields = new String[] { "num int", "words text" };

		String dataPath = hdfsWorkingFolder + "/dataWithErr.txt";

		Table dataTable = new Table("dataTable", null);

		dataTable.addRow(new String[] { "All Together Now", "The Beatles" });
		dataTable.addRow(new String[] { "1", "one" });
		dataTable.addRow(new String[] { "2", "two" });
		dataTable.addRow(new String[] { "3", "three" });
		dataTable.addRow(new String[] { "4", "four" });
		dataTable.addRow(new String[] { "can", "I" });
		dataTable.addRow(new String[] { "have", "a" });
		dataTable.addRow(new String[] { "little", "more" });
		dataTable.addRow(new String[] { "5", "five" });
		dataTable.addRow(new String[] { "6", "six" });
		dataTable.addRow(new String[] { "7", "seven" });
		dataTable.addRow(new String[] { "8", "eight" });
		dataTable.addRow(new String[] { "9", "nine" });
		dataTable.addRow(new String[] { "10", "ten - I love you!" });

		hdfs.writeTableToFile(dataPath, dataTable, ",");

		ErrorTable errorTable = new ErrorTable("err_table");
		hawq.runQueryWithExpectedWarning(errorTable.constructDropStmt(true), "(drop cascades to external table err_table_test|" + "table \"" + errorTable.getName() + "\" does not exist, skipping)", true, true);

		exTable = TableFactory.getPxfReadableTextTable("err_table_test", fields, dataPath, ",");
		exTable.setErrorTable(errorTable.getName());
		exTable.setSegmentRejectLimit(10);

		ReportUtils.startLevel(report, getClass(), "Drop and create table, expect error table notice");
		hawq.dropTable(exTable, false);
		hawq.runQueryWithExpectedWarning(exTable.constructCreateStmt(), "Error table \"" + errorTable.getName() + "\" does not exist. " + "Auto generating an error table with the same name", true, true);

		Assert.assertTrue(hawq.checkTableExists(exTable));
		ReportUtils.stopLevel(report);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num ASC");

		hawq.verifyWarning("Found 4 data formatting errors \\(4 or more input rows\\). " + "Rejected related input data.", true, false);

		// remove erroneous rows
		ReportUtils.report(report, getClass(), "Remove erroneous rows from dataTable");
		List<List<String>> data = dataTable.getData();
		ListIterator<List<String>> iter = data.listIterator();
		while (iter.hasNext()) {
			List<String> element = iter.next();
			if (!StringUtils.isNumeric(element.get(0))) {
				iter.remove();
			}
		}
		dataTable.setData(data);

		ComparisonUtils.compareTables(exTable, dataTable, report);

		hawq.queryResults(errorTable, "SELECT relname, filename, linenum, errmsg, rawdata FROM " + errorTable.getName() + " ORDER BY cmdtime ASC");

		// Temporary workaround for lack of public getLocationUri() method
		// Opened GPSQL-1798 under PXF Test Automation to clean this up.
		String createStmt = exTable.constructCreateStmt();
		String locationUri = createStmt.substring(createStmt.indexOf("pxf://"), createStmt.indexOf("')"));

		Table errData = new Table("errData", null);
		errData.addRow(new String[] {
				exTable.getName(),
				locationUri,
				"1",
				"invalid input syntax for integer: \"All Together Now\", column num",
				"All Together Now,The Beatles" });
		errData.addRow(new String[] {
				exTable.getName(),
				locationUri,
				"6",
				"invalid input syntax for integer: \"can\", column num",
				"can,I" });
		errData.addRow(new String[] {
				exTable.getName(),
				locationUri,
				"7",
				"invalid input syntax for integer: \"have\", column num",
				"have,a" });
		errData.addRow(new String[] {
				exTable.getName(),
				locationUri,
				"8",
				"invalid input syntax for integer: \"little\", column num",
				"little,more" });

		ComparisonUtils.compareTables(errorTable, errData, report);

		ReportUtils.startLevel(report, getClass(), "table with too many errors");
		exTable.setSegmentRejectLimit(3);
		hawq.createTableAndVerify(exTable);

		try {
			hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num ASC");
		} catch (PSQLException e) {
			PSQLException expected = new PSQLException("Segment reject limit reached. Aborting operation. " + "Last error was: invalid input syntax for integer: \"have\", column num", null);
			ExceptionUtils.validate(report, e, expected, true);
		}

		ReportUtils.stopLevel(report);
	}

	/**
	 * Verify pg_remote_credentials exists and created with the expected structure
	 * 
	 * @throws Exception
	 */
	@Test
	public void remoteCredentialsCatalogTable() throws Exception {
		Table results = new Table("results", null);
		hawq.queryResults(results, "SELECT * FROM pg_remote_credentials");

		Table expected = new Table("expected", null);
		expected.addColumn("rcowner", Types.BIGINT);
		expected.addColumn("rcservice", Types.VARCHAR);
		expected.addColumn("rcremoteuser", Types.VARCHAR);
		expected.addColumn("rcremotepassword", Types.VARCHAR);

		ComparisonUtils.compareTablesMetadata(expected, results);
		ComparisonUtils.compareTables(results, expected, report);
	}

	/**
	 * Verify pg_remote_logins exists, created with the expected structure and does not print any
	 * passwords
	 * 
	 * pg_remote_logins is a view ontop pg_remote_credentials.
	 * 
	 * @throws Exception
	 */
	@Test
	public void remoteLoginsView() throws Exception {

		// SETUP
		hawq.runQuery("SET allow_system_table_mods = 'DML';");
		hawq.runQuery("INSERT INTO pg_remote_credentials VALUES (10, 'a', 'b', 'c');");

		// TEST
		Table results = new Table("results", null);
		hawq.queryResults(results, "SELECT * FROM pg_remote_logins");

		// COMPARISON
		Table expected = new Table("expected", null);
		expected.addColumn("rolname", Types.VARCHAR);
		expected.addColumn("rcservice", Types.VARCHAR);
		expected.addColumn("rcremoteuser", Types.VARCHAR);
		expected.addColumn("rcremotepassword", Types.VARCHAR);
		expected.addRow(new String[] {
				System.getProperty("user.name"),
				"a",
				"b",
				"********" });

		ComparisonUtils.compareTablesMetadata(expected, results);
		ComparisonUtils.compareTables(results, expected, report);

		// CLEANUP
		hawq.runQuery("DELETE FROM pg_remote_credentials WHERE rcowner = 10;");
		hawq.runQuery("SET allow_system_table_mods = 'NONE';");
	}

	/**
	 * Verify pg_remote_credentials has the correct ACLs
	 * 
	 * @throws Exception
	 */
	@Test
	public void remoteCredentialsACL() throws Exception {

		// TEST
		Table results = new Table("results", null);
		hawq.queryResults(results, "SELECT relacl FROM pg_class WHERE relname = 'pg_remote_credentials'");

		// COMPARISON
		Table expected = new Table("expected", null);
		expected.addColumnHeader("relacl");
		expected.addColDataType(Types.ARRAY);
		expected.addRow(new String[] { "{" + System.getProperty("user.name") + "=arwdxt/" + System.getProperty("user.name") + "}" });

		ComparisonUtils.compareTablesMetadata(expected, results);
		ComparisonUtils.compareTables(results, expected, report);
	}

	/**
	 * Namenode High-availability test - creating table with non-existent nameservice
	 * 
	 * @throws Exception
	 */
	@Test
	public void negativeHaNameserviceNotExist() throws Exception {
		String unknownNameservicePath = "text_data.csv";

		exTable = TableFactory.getPxfReadableTextTable("hatable", new String[] {
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
				"n17 int" }, (unknownNameservicePath), ",");

		exTable.setHost("phdcluster");
		exTable.setPort(null);

		try {
			hawq.createTableAndVerify(exTable);
		} catch (Exception e) {
			ExceptionUtils.validate(report, e, new PSQLException("ERROR: nameservice phdcluster not found in client configuration. No HA namenodes provided", null), false);
		}
	}

	/*
	 * Verify use of LIMIT
	 *
	 * TODO The test doesn't verify whether Hawq got all tuples 
	 * or just the LIMIT. We should test LIMIT cancels the query once 
	 * it gets LIMIT tuples.
	 */
	@Test
	public void queryLimit() throws Exception {
		
		String filePath = hdfsWorkingFolder + "/text_limit.txt";
		
		Table dataTable = new Table("dataTable", null);
		Table limitTable = new Table("limitTable", null);
		String[] line = new String[] {"Same shirt", "different day"};
		for (int i = 0; i < 1000; i++) {
			limitTable.addRow(line);
		}
		for (int i = 0; i < 10; i++) {
			dataTable.appendRows(limitTable);
		}
		
		hdfs.writeTableToFile(filePath, dataTable, ",");

		String[] fields = new String[] {
				"s1 text",
				"s2 text"};
		exTable = TableFactory.getPxfReadableTextTable("text_limit", fields, filePath, ",");

		hawq.createTableAndVerify(exTable);

		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " LIMIT 1000");

		ComparisonUtils.compareTables(exTable, limitTable, report);
	}
	
	/*
	 * Verify query fails when conversion to int (for example) fails 
	 * (without an error table) and a proper error message is printed
	 */
	@Test 
	public void negativeBadTextData() throws Exception {
		
		String filePath = hdfsWorkingFolder + "/bad_text.txt";
		
		Table dataTable = new Table("dataTable", null);
		for (int i = 0; i < 50; i++) {
			dataTable.addRow(new String[] {"" + i, Integer.toHexString(i)});
		}
		dataTable.addRow(new String[] {"joker", "ha"});
		
		hdfs.writeTableToFile(filePath, dataTable, ",");

		String[] fields = new String[] {
				"num int",
				"string text"};
		exTable = TableFactory.getPxfReadableTextTable("bad_text", fields, filePath, ",");

		hawq.createTableAndVerify(exTable);
		
		try {
			hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " ORDER BY num");
		} catch (PSQLException e) {
			String err = "ERROR: invalid input syntax for integer: \"joker\"";
			ExceptionUtils.validate(report, e, new PSQLException(err, null), true);
		}
		
	}
	
	private Table prepareRecursiveDirsData(String baseDir, String delim) throws Exception {

		Table fileTable = new Table("file", null);
		Table dataTable = new Table("dataTable", null);

		/*
		 * - recursive/ -level1leaf1/ file1 -level1leaf2/ -level2leaf1/ file2 file3 -level3/ file4
		 * -level2leaf2/
		 */
		hdfs.createDirectory(baseDir);
		hdfs.createDirectory(baseDir + "/level1leaf1");
		hdfs.createDirectory(baseDir + "/level1leaf2");
		hdfs.createDirectory(baseDir + "/level1leaf2/level2leaf1");
		hdfs.createDirectory(baseDir + "/level1leaf2/level2leaf1/level3");
		hdfs.createDirectory(baseDir + "/level1leaf2/level2leaf2");

		fileTable.addRow(new String[] { "When the truth is found to be lies", "line1" });
		fileTable.addRow(new String[] { "And all the joy within you dies", "line2" });
		hdfs.writeTableToFile(baseDir + "/level1leaf1/file1", fileTable, delim);
		dataTable.appendRows(fileTable);
		fileTable.setData(null);

		fileTable.addRow(new String[] {
				"Don't you want somebody to love, don't you",
				"line3" });
		fileTable.addRow(new String[] { "Need somebody to love, wouldn't you", "line4" });
		hdfs.writeTableToFile(baseDir + "/level1leaf2/level2leaf1/file2", fileTable, delim);
		dataTable.appendRows(fileTable);
		fileTable.setData(null);

		fileTable.addRow(new String[] { "Love somebody to love, you better", "line5" });
		hdfs.writeTableToFile(baseDir + "/level1leaf2/level2leaf1/file3", fileTable, delim);
		dataTable.appendRows(fileTable);
		fileTable.setData(null);

		fileTable.addRow(new String[] { "Find somebody to love", "line6" });
		hdfs.writeTableToFile(baseDir + "/level1leaf2/level2leaf1/level3/file4", fileTable, delim);
		dataTable.appendRows(fileTable);
		fileTable.setData(null);

		return dataTable;
	}

	private void verifyFilterPushdown(ReadableExternalTable printFilterTable, 
			String queryFilter, String serializedFilter, int rows)
			throws Exception {
		
		final String NO_FILTER = "No filter";

		ReportUtils.startLevel(report, getClass(), "run query with filter '" + queryFilter + "'");
		try {
			hawq.queryResults(printFilterTable, "SELECT s1 FROM " + printFilterTable.getName() + " " + queryFilter + ";");
			Assert.fail("Query should throw a filter printer exception");
		} catch (Exception e) {
			// tcServer displays special character in their HTML representation, 
			// need to convert our string as well.
			if (serializedFilter == null) {
				serializedFilter = NO_FILTER;
			}
			serializedFilter = StringEscapeUtils.escapeHtml(serializedFilter);
			ExceptionUtils.validate(report, e, new Exception("ERROR.*Filter string: '" + serializedFilter + "'.*"), true, true);
		}
		
		ReportUtils.report(report, getClass(), "expecting query to return " + rows + " rows");
		hawq.queryResults(exTable, "SELECT * FROM " + exTable.getName() + " " + queryFilter + ";");
		Assert.assertEquals(exTable.getData().size(), rows);
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

	@Test
	public void textFormatDeprecatedClasses() throws Exception {

		String csvPath = hdfsWorkingFolder + "/text_data.csv";

		Table dataTable = new Table("dataTable", null);

		FileFormatsUtils.prepareData(new CustomTextPreparer(), 1000, dataTable);

		hdfs.writeTableToFile(csvPath, dataTable, ",");

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
		exTable.setAccessor("TextFileAccessor");
		exTable.setResolver("TextResolver");
		exTable.setAnalyzer("HdfsAnalyzer");
		exTable.setDelimiter(",");

		try {
			hawq.createTableAndVerify(exTable);
			Assert.fail("A SQLWarning should have been thrown");
		} catch (SQLWarning warnings) {
			SQLWarning warning = warnings;
			assertUseIsDeprecated("HdfsDataFragmenter", warning);
			warning = warning.getNextWarning();
			assertUseIsDeprecated("TextFileAccessor", warning);
			warning = warning.getNextWarning();
			assertUseIsDeprecated("TextResolver", warning);
			warning = warning.getNextWarning();
			assertUseIsDeprecated("HdfsAnalyzer", warning);
			warning = warning.getNextWarning();
			Assert.assertNull(warning);
		}

		// TODO once jsystem-infra supports throwing warnings from queryResults
		// check warnings are also printed here
		hawq.queryResults(exTable, "SELECT * FROM bigtext ORDER BY n1");

		ComparisonUtils.compareTables(exTable, dataTable, report);
		try {
			verifyAnalyze(exTable);
			Assert.fail("A SQLWarning should have been thrown");
		} catch (SQLWarning warnings) {
			SQLWarning warning = warnings;
			assertUseIsDeprecated("HdfsDataFragmenter", warning);
			warning = warning.getNextWarning();
			assertUseIsDeprecated("TextFileAccessor", warning);
			warning = warning.getNextWarning();
			assertUseIsDeprecated("TextResolver", warning);
			warning = warning.getNextWarning();
			assertUseIsDeprecated("HdfsAnalyzer", warning);
			warning = warning.getNextWarning();
			Assert.assertNull(warning);
		}
	}

	private void assertUseIsDeprecated(String classname, SQLWarning warning) {
		Assert.assertEquals("Use of " + classname + " is deprecated and it will be removed on the next major version", warning.getMessage());
	}
}
