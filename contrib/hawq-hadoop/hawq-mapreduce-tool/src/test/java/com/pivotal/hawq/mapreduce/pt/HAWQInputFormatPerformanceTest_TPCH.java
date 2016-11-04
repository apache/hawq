package com.pivotal.hawq.mapreduce.pt;

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


import com.google.common.collect.Lists;
import com.pivotal.hawq.mapreduce.*;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import com.pivotal.hawq.mapreduce.util.HAWQJdbcUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * MapReduce driver class to do manually performance tests on reading TPC-H table.
 *
 * Usage:
 * 1. load and query
 * HAWQInputFormatPerformanceTest_TPCH ao|parquet scale is_partition tableName [columns]
 * 2. query-only
 * HAWQInputFormatPerformanceTest_TPCH --query-only tableName [columns]
 *
 * Example:
 * $ hadoop com.pivotal.hawq.mapreduce.pt.HAWQInputFormatPerformanceTest_TPCH ao 5 false lineitem_ao_row L_PARTKEY,L_COMMENT
 */
public class HAWQInputFormatPerformanceTest_TPCH extends Configured implements Tool {

	static class TPCHTableMapper extends Mapper<Void, HAWQRecord, Text, Void> {
		@Override
		protected void map(Void key, HAWQRecord value, Context context)
				throws IOException, InterruptedException {
			try {
				String recordString = toRecordString(value);
				context.write(new Text(recordString), null);

			} catch (HAWQException e) {
				throw new IOException(e);
			}
		}

		private String toRecordString(HAWQRecord record) throws HAWQException {
			HAWQSchema schema = record.getSchema();

			// read all columns if user didn't specify column list
			if (colNames == null) {
				StringBuilder buf = new StringBuilder(toFieldString(record, 1));
				for (int i = 2; i <= schema.getFieldCount(); i++) {
					buf.append("|").append(toFieldString(record, i));
				}
				return buf.toString();
			}

			assert colNames.size() > 0;
			StringBuilder buf = new StringBuilder(toFieldString(record, schema.getFieldIndex(colNames.get(0))));
			for (int i = 1; i < colNames.size(); i++) {
				buf.append("|").append(toFieldString(record, schema.getFieldIndex(colNames.get(i))));
			}
			return buf.toString();
		}

		private String toFieldString(HAWQRecord record, int fieldIndex)
				throws HAWQException {
			Object val = record.getObject(fieldIndex);
			if (val == null) return "null";
			if (val instanceof byte[]) return new String((byte[]) val);
			return val.toString();
		}
	}

	// command line arguments
	boolean queryOnly;
	HAWQTableFormat tableFormat;
	String scale;
	boolean isPartition;
	String tableName;
	static List<String> colNames;	// used by TPCHTableMapper to determine which column to read.

	// counters
	long dataLoadTime;
	long metadataExtractTime;
	long mapReduceTime;

	private boolean readArguments(String[] args) {
		if (args.length == 0)
			return false;

		int tableNameArgIndex;
		queryOnly = args[0].equalsIgnoreCase("--query-only");

		if (queryOnly) {
			if (args.length != 2 && args.length != 3)
				return false;
			tableNameArgIndex = 1;

		} else {
			if (args[0].equalsIgnoreCase("ao"))
				tableFormat = HAWQTableFormat.AO;
			else if (args[0].equalsIgnoreCase("parquet"))
				tableFormat = HAWQTableFormat.Parquet;
			else
				return false;

			if (args.length != 4 && args.length != 5)
				return false;

			scale = args[1];
			isPartition = Boolean.parseBoolean(args[2]);
			tableNameArgIndex = 3;
		}

		tableName = args[tableNameArgIndex];
		if (tableNameArgIndex + 1 < args.length) {
			colNames = Lists.newArrayList(args[tableNameArgIndex + 1].split(","));
		}
		return true;
	}

	private void loadTPCHData() throws Exception {
		// get number of segments
		int segnum;
		Connection conn = null;
		try {
			conn = MRFormatTestUtils.getTestDBConnection();
			Map<String, String> rs = HAWQJdbcUtils.executeSafeQueryForSingleRow(
					conn, "SELECT COUNT(*) segnum FROM gp_segment_configuration WHERE role='p';");
			segnum = Integer.parseInt(rs.get("segnum"));
		} finally {
			HAWQJdbcUtils.closeConnection(conn);
		}

		// run external script to load TPC-H data
		TPCHTester.HAWQTPCHSpec spec = new TPCHTester.HAWQTPCHSpec(scale, tableFormat, isPartition);
		MRFormatTestUtils.runShellCommand(spec.getLoadCmd(segnum));
	}
	
	private int runMapReduceJob() throws Exception {
		Path outputPath = new Path("/output");
		// delete previous output
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		fs.close();

		Job job = new Job(getConf());
		job.setJarByClass(HAWQInputFormatPerformanceTest_TPCH.class);

		job.setInputFormatClass(HAWQInputFormat.class);

		long startTime = System.currentTimeMillis();
		HAWQInputFormat.setInput(job.getConfiguration(), MRFormatConfiguration.TEST_DB_URL, null, null, tableName);
		metadataExtractTime = System.currentTimeMillis() - startTime;

		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(TPCHTableMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Void.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (!readArguments(args)) {
			System.err.printf("Usage: %s [generic options] <ao|parquet> <scale> <is_partition> <tableName> [<col1,col2>]\n",
							  getClass().getSimpleName());
			System.err.printf("       %s [generic options] --query-only <tableName> [<col1,col2>]\n",
							  getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		long startTime;

		if (!queryOnly) {
			startTime = System.currentTimeMillis();
			loadTPCHData();
			dataLoadTime = System.currentTimeMillis() - startTime;
		}

		startTime = System.currentTimeMillis();
		int res = runMapReduceJob();
		mapReduceTime = System.currentTimeMillis() - startTime;

		System.out.println("=====================================");
		System.out.println("========= Reports ===================");
		System.out.println("Table read : " + tableName);
		if (!queryOnly) {
		System.out.println("Data volume:        " + scale + "G");
		System.out.println("TPC-H Data Loading: " + dataLoadTime + "ms");
		}
		System.out.println("Metadata Extract:   " + metadataExtractTime + "ms");
		System.out.println("MapReduce Job:      " + mapReduceTime + "ms");
		return res;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HAWQInputFormatPerformanceTest_TPCH(), args);
		System.exit(exitCode);
	}
}
