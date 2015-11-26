package com.pivotal.hawq.mapreduce;

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


import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.pivotal.hawq.mapreduce.util.HAWQJdbcUtils;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import static com.pivotal.hawq.mapreduce.MRFormatConfiguration.FT_TEST_FOLDER;

/**
 * Test a simple table using cluster job runner, suitable for Integration Test.
 */
public class SimpleTableClusterTester extends SimpleTableTester {
	protected static final Path HDFS_OUTPUT_PATH = new Path("/temp/hawqinputformat");

	@Override
	protected void testSimpleTable(HAWQTable table, Class<? extends Mapper> mapperClass) throws Exception {
		final String tableName = table.getTableName();
		final String caseName = tableName.replaceAll("\\.", "_");
		System.out.println("Executing test case: " + caseName);

		final File caseFolder	= new File(FT_TEST_FOLDER, caseName);
		final File sqlFile		= new File(caseFolder, caseName + ".sql");
		final File answerFile	= new File(caseFolder, caseName + ".ans");
		final File outputFile	= new File(caseFolder, caseName + ".out");

		if (caseFolder.exists()) {
			FileUtils.deleteDirectory(caseFolder);
		}
		caseFolder.mkdir();

		Connection conn = null;
		List<String> answers;
		try {
			conn = MRFormatTestUtils.getTestDBConnection();

			// 1. prepare test data
			String setupSQLs = table.generateDDL() + table.generateData();
			Files.write(setupSQLs.getBytes(), sqlFile);
			MRFormatTestUtils.runSQLs(conn, setupSQLs);

			// 2. generate answer
			answers = MRFormatTestUtils.dumpTable(conn, tableName);
			Collections.sort(answers);
			Files.write(Joiner.on('\n').join(answers).getBytes(), answerFile);

		} finally {
			HAWQJdbcUtils.closeConnection(conn);
		}

		// 3. run input format driver
		int exitCode = MRFormatTestUtils.runMapReduceOnCluster(tableName, HDFS_OUTPUT_PATH, mapperClass);
		Assert.assertEquals(0, exitCode);

		// 4. copy hdfs output to local
		final Path hdfsOutputFile = new Path(HDFS_OUTPUT_PATH, "part-r-00000");
		MRFormatTestUtils.runShellCommand(
				String.format("hadoop fs -copyToLocal %s %s",
							  hdfsOutputFile.toString(), outputFile.getPath()));

		// 5. compare result
		List<String> outputs = Files.readLines(outputFile, Charsets.UTF_8);
		checkOutput(answers, outputs, table);

		System.out.println("Successfully finish test case: " + caseName);
	}
}
