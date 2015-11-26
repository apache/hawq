package com.pivotal.hawq.mapreduce.parquet;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

public class TestHAWQParquetOutput {
	private static final PageReadStore NULL = null;
	private ParquetMetadata readFooter;
	private ParquetFileReader reader;
	private Configuration configuration;
	private Path path;
	private MessageType schema;

	public TestHAWQParquetOutput(String filepath) throws Exception {
		File parquetFile = new File(filepath).getAbsoluteFile();
		path = new Path(parquetFile.toURI());
		configuration = new Configuration();
		readFooter = ParquetFileReader.readFooter(configuration, path);
		schema = readFooter.getFileMetaData().getSchema();
	}

	public int getRowGroupCount() {
		int count = readFooter.getBlocks().size();
		System.out.println("-------------RowGroup Count: " + count);
		return count;
	}

	public long getRowCount(int rowGroupIndex) {
		long count = readFooter.getBlocks().get(rowGroupIndex).getRowCount();
		System.out.println("-------------Row Count in RowGroup" + rowGroupIndex
				+ ": " + count);
		return count;
	}

	public int getColumnCountInRowGroup(int rowGroupIndex) {
		int count = readFooter.getBlocks().get(rowGroupIndex).getColumns()
				.size();
		System.out.println("-------------Column Count in RowGroup"
				+ rowGroupIndex + ": " + count);
		return count;
	}

	public void getColumnType(int rowGroupIndex, int columnIndex) {
		String type = readFooter.getBlocks().get(rowGroupIndex).getColumns()
				.get(columnIndex).getType().toString();
		System.out.println("-------------Type of Column" + columnIndex
				+ " in RowGroup" + rowGroupIndex + ": " + type);
	}

	public void getColumnCodec(int rowGroupIndex, int columnIndex) {
		String Codec = readFooter.getBlocks().get(rowGroupIndex).getColumns()
				.get(columnIndex).getCodec().toString();
		System.out.println("-------------Codec of Column" + columnIndex
				+ " in RowGroup" + rowGroupIndex + ": " + Codec);
	}

	public void getMetadata() {
		String metadata = readFooter.toString();
		System.out.println(metadata);
	}

	public void getColumnDataInRowGroup(int rowGroupIndex, int columnIndex)
			throws Exception {
		PageReadStore pages = NULL;
		int valueCount;
		reader = new ParquetFileReader(configuration, path,
				readFooter.getBlocks(), schema.getColumns());
		for (int i = 0; i <= rowGroupIndex; ++i) {
			pages = reader.readNextRowGroup();
		}
		PageReader pageReader = pages.getPageReader(schema.getColumns().get(
				columnIndex));
		Page page = pageReader.readPage();
		
		/*read out all the data pages*/
		while(page != NULL)
		{
			valueCount = page.getValueCount();
			System.out.println("-------------value count of Column" + columnIndex
					+ " in RowGroup" + rowGroupIndex + ": " + valueCount);
			printIntFromByteArray(page.getBytes().toByteArray());
			System.out.println("");
			page = pageReader.readPage();
		}
		reader.close();
	}

	public void printIntFromByteArray(byte[] bytes) {
		int len = bytes.length;

		int number = len / 4;
		for (int i = 0; i < number; i++) {
			byte[] intByteRes = { bytes[i * 4], bytes[4 * i + 1],
					bytes[4 * i + 2], bytes[4 * i + 3] };
			System.out.print(fromByteArray(intByteRes) + "\t");
		}
	}

	int fromByteArray(byte[] bytes) {
		return bytes[3] << 24 | (bytes[2] & 0xFF) << 16
				| (bytes[1] & 0xFF) << 8 | (bytes[0] & 0xFF);
	}

	public static void main(String[] args) throws Exception {
		System.out.println("test parquet");

		TestHAWQParquetOutput b = new TestHAWQParquetOutput(
//				"/Users/bjcoe/parquet_tabledata/pa_seg1");
				"/Users/gaod1/Perforce/gaod-hawq/gpsql/feature/hawq/cdb-pg/contrib/hawq-hadoop/hawq-mapreduce-parquet/input/simple/16399.1");
//				"/Users/bjcoe/286730.1");//196608.1");			//required one column table
//				"/Users/bjcoe/221199.1");//212992.1");			//required two columns table
//				"/Users/bjcoe/221204.1");			//required three columns table
//	"/Users/bjcoe/212997.1");			//required two columns table
//				"/Users/bjcoe/172037.1");				//one column table
//				"/Users/bjcoe/180224.1");		//two columns table
	
		/* print metadata part*/
		b.getMetadata();
		
		/* print actual data*/
		int rowGroupNo = b.readFooter.getBlocks().size();
		for(int i = 0; i < rowGroupNo; i++)
		{
			BlockMetaData blockMetadata = b.readFooter.getBlocks().get(i);
			int columnNo = blockMetadata.getColumns().size();
			for(int j = 0; j < columnNo; j++)
			{
				b.getColumnDataInRowGroup(i, j);
			}
		}
	}
}
