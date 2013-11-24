package com.pxf.examples.hdfs;

import java.io.File;

import junit.framework.SystemTestCase4;

import org.junit.Before;
import org.junit.Test;

import com.pxf.infra.hdfs.Hdfs;
import com.pxf.infra.structures.tables.basic.Table;
import com.pxf.infra.utils.fileformats.FileFormatsUtils;
import com.pxf.infra.utils.fileformats.avro.schema.CustomAvroRecInSequence;
import com.pxf.infra.utils.fileformats.avro.schema.CustomrAvroInSequenceReader;
import com.pxf.infra.utils.fileformats.avro.schema.CustomrAvroReader;
import com.pxf.infra.utils.fileformats.avro.schema.IAvroSchema;

public class HdfsTests extends SystemTestCase4 {

	Hdfs hdfs;

	@Before
	public void setUp() throws Exception {
		hdfs = (Hdfs) system.getSystemObject("hdfs");
	}

	@Test
	public void fofo() throws Exception {
		hdfs.getFunc().createDirectory("blabla");
	}

	@Test
	public void copyFiles() throws Exception {

		String newDir = "user/koazu/myTestDir";

		hdfs.getFunc().list("user/koazu");

		hdfs.getFunc().removeDirectory(newDir);
		hdfs.getFunc().createDirectory(newDir);

		hdfs.getFunc().list("user/koazu");

		File localFile1 = new File("resources/osx-gcc-4.4.2.tar");
		File localFile2 = new File("resources/README.bla");

		hdfs.getFunc()
				.copyFromLocal(localFile1.getAbsolutePath(), newDir + "/osx-gcc-4.4.2.tar");

		hdfs.getFunc()
				.copyFromLocal(localFile2.getAbsolutePath(), newDir + "/README.bla");

		hdfs.getFunc().getFileContent(newDir + "/README.bla");
	}

	// @Test
	// public void writeSequence() throws Exception {
	// CustomWritable[] cwArr = new CustomWritable[10];
	//
	// for (int i = 0; i < cwArr.length; i++) {
	// int num1 = i;
	//
	// Timestamp tms = new Timestamp(Calendar.getInstance()
	// .getTime()
	// .getTime());
	//
	// cwArr[i] = new CustomWritable(tms, num1, 10 * num1, 20 * num1);
	//
	// }
	//
	// hdfs.func.writeSequnceFile(cwArr, "myBlaBla/ggggg.tblulu");
	//
	// System.out.println(hdfs.func.list("myBlaBla/ggggg.tblulu"));
	//
	// }

	@Test
	public void writeAvro() throws Exception {

		Table dataTable = new Table("dataTable", null);

		String schemaName = "regression/resources/regressPXFCustomAvro.avsc";

		Object[] data = FileFormatsUtils.prepareData(new CustomrAvroReader(schemaName), 1000, dataTable);

		hdfs.getFunc()
				.writeAvroFile("avro_data/bla.avro", schemaName, (IAvroSchema[]) data);
	}

	@Test
	public void writeAvroInSequence() throws Exception {

		Table dataTable = new Table("dataTable", null);

		String schemaName = "regression/resources/regressPXFCustomAvro.avsc";

		Object[] data = FileFormatsUtils.prepareData(new CustomrAvroInSequenceReader(schemaName), 10, dataTable);

		hdfs.getFunc()
				.writeAvroInSequnceFile("avro_data/avro_in_seq.tbl", schemaName, (IAvroSchema[]) data);
	}
}