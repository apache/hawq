package com.pxf.infra.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;

import jsystem.framework.system.SystemObjectImpl;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;

import com.pxf.infra.utils.fileformats.avro.schema.IAvroSchema;
import com.pxf.infra.utils.jsystem.report.ReportUtils;
import com.pxf.infra.utils.jsystem.sut.SutUtils;

/**
 * implementation of HdfsFunctionality using HDFS Java API interface,
 */
public class HdfsFunctionalityJavaApi extends SystemObjectImpl implements
		HdfsFunctionality {

	private FileSystem fs;
	private Configuration config;
	private String host;
	private String port = "8020";

	@Override
	public void init() throws Exception {

		super.init();

		config = new Configuration();

		host = SutUtils.getValue(sut, "//hdfs/host");

		String sutPort = SutUtils.getValue(sut, "//hdfs/port");

		if (sutPort != null && !sutPort.equals("null") && !sutPort.equals("")) {
			port = sutPort;
		}

		config.set("fs.defaultFS", "hdfs://" + host + ":" + port + "/");

		fs = FileSystem.get(config);
	}

	@Override
	public ArrayList<String> list(String path) throws Exception {

		ReportUtils.startLevel(report, getClass(), "List From " + path);

		RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("/" + path), true);

		ArrayList<String> filesList = new ArrayList<String>();

		while (list.hasNext()) {

			filesList.add(list.next().getPath().toString());
		}

		ReportUtils.report(report, getClass(), filesList.toString());
		ReportUtils.stopLevel(report);

		return filesList;
	}

	@Override
	public int listSize(String hdfsDir) throws Exception {

		return list(hdfsDir).size();
	}

	@Override
	public void copyFromLocal(String srcPath, String destPath) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Copy From " + srcPath + " to " + destPath);

		fs.copyFromLocalFile(new Path(srcPath), new Path("/" + destPath));

		ReportUtils.stopLevel(report);
	}

	@Override
	public void createDirectory(String path) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Create Directory " + path);

		fs.mkdirs(new Path("/" + path));

		ReportUtils.stopLevel(report);
	}

	@Override
	public void removeDirectory(String path) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Remove Directory " + path);

		fs.delete(new Path("/" + path), true);

		ReportUtils.stopLevel(report);
	}

	@Override
	public String getFileContent(String path) throws Exception {

		ReportUtils.startLevel(report, getClass(), "Get file content");

		FSDataInputStream fsdis = fs.open(new Path("/" + path));

		StringWriter writer = new StringWriter();
		IOUtils.copy(fsdis, writer, "UTF-8");

		ReportUtils.report(report, getClass(), writer.toString());

		ReportUtils.stopLevel(report);

		return writer.toString();
	}

	public void writeSequnceFile(Object[] writableData, String pathToFile)
			throws IOException {

		ReportUtils.startLevel(report, getClass(), "Writing Sequence file from " + writableData[0].getClass()
				.getName() + " array to " + pathToFile);

		IntWritable key = new IntWritable();

		Path path = new Path("/" + pathToFile);

		Writer.Option optPath = SequenceFile.Writer.file(path);
		Writer.Option optKey = SequenceFile.Writer.keyClass(key.getClass());
		Writer.Option optVal = SequenceFile.Writer.valueClass(writableData[0].getClass());

		SequenceFile.Writer writer = SequenceFile.createWriter(config, optPath, optKey, optVal);

		for (int i = 1; i < writableData.length; i++) {

			writer.append(key, writableData[i]);
		}

		writer.close();

		ReportUtils.stopLevel(report);

	}

	public void writeAvroInSequnceFile(String pathToFile, String schemaName, IAvroSchema[] data)
			throws Exception {

		IntWritable key = new IntWritable();
		BytesWritable val = new BytesWritable();
		Path path = new Path("/" + pathToFile);

		Writer.Option optPath = SequenceFile.Writer.file(path);
		Writer.Option optKey = SequenceFile.Writer.keyClass(key.getClass());
		Writer.Option optVal = SequenceFile.Writer.valueClass(val.getClass());

		SequenceFile.Writer writer = SequenceFile.createWriter(config, optPath, optKey, optVal);

		for (int i = 0; i < data.length; i++) {

			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			data[i].serialize(stream);
			val = new BytesWritable(stream.toByteArray());
			writer.append(key, val);
		}

		writer.close();
	}

	public void writeAvroFile(String pathToFile, String schemaName, IAvroSchema[] data)
			throws Exception {

		Path path = new Path("/" + pathToFile);
		OutputStream outStream = fs.create(path);

		Schema schema = new Schema.Parser().parse(new FileInputStream(schemaName));
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		dataFileWriter.create(schema, outStream);

		for (int i = 0; i < data.length; i++) {

			GenericRecord datum = data[i].serialize();
			dataFileWriter.append(datum);
		}

		dataFileWriter.close();
	}

}