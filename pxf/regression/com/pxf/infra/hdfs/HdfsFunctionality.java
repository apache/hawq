package com.pxf.infra.hdfs;

import java.io.IOException;
import java.util.ArrayList;

import jsystem.framework.system.SystemObject;

import com.pxf.infra.utils.fileformats.avro.schema.IAvroSchema;

public interface HdfsFunctionality extends SystemObject {

	public ArrayList<String> list(String path) throws Exception;

	public void copyFromLocal(String stcPath, String destPath) throws Exception;

	public void createDirectory(String path) throws Exception;

	public void removeDirectory(String path) throws Exception;

	public String getFileContent(String path) throws Exception;

	public int listSize(String hdfsDir) throws Exception;

	public void writeSequnceFile(Object[] writableData, String pathToFile)
			throws IOException;

	public void writeAvroFile(String pathToFile, String schemaName, IAvroSchema[] data)
			throws Exception;

	public void writeAvroInSequnceFile(String pathToFile, String schemaName, IAvroSchema[] data)
			throws Exception;
}
