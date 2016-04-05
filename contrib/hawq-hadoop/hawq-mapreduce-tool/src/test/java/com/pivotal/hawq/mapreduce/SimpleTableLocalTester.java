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

import static com.pivotal.hawq.mapreduce.MRFormatConfiguration.TEST_DB_NAME;
import static com.pivotal.hawq.mapreduce.MRFormatConfiguration.TEST_FOLDER;

/**
 * Test a simple table using local job runner, suitable for Unit Test.
 */
public class SimpleTableLocalTester extends SimpleTableTester {

	@Override
	protected void testSimpleTable(HAWQTable table, Class<? extends Mapper> mapperClass) throws Exception {
		final String tableName = table.getTableName();
		// use table name as case name
		final File caseFolder	= new File(TEST_FOLDER, tableName);
		final File sqlFile		= new File(caseFolder, tableName + ".sql");
		final File answerFile	= new File(caseFolder, tableName + ".ans");
		final File metadataFile	= new File(caseFolder, tableName + ".yaml");
		final File outputFile	= new File(caseFolder, "output/part-r-00000");
		final String caseName = tableName.replaceAll("\\.", "_");
		System.out.println("Executing test case: " + caseName);

		List<String> answers;

		if (caseFolder.exists()) {
			answers = Files.readLines(answerFile, Charsets.UTF_8);

		} else {
			// generate test data and corresponding answer file and metadata file
			// if case folder not exist
			caseFolder.mkdir();

			Connection conn = null;
			try {
				conn = MRFormatTestUtils.getTestDBConnection();

				// prepare test data
				String setupSQLs = table.generateDDL() + table.generateData();
				Files.write(setupSQLs.getBytes(), sqlFile);
				MRFormatTestUtils.runSQLs(conn, setupSQLs);

				// generate answer
				answers = MRFormatTestUtils.dumpTable(conn, tableName);
				Collections.sort(answers);
				Files.write(Joiner.on('\n').join(answers).getBytes(), answerFile);

				// extract metadata
				MRFormatTestUtils.runShellCommand(
						String.format("hawqextract -d %s -o %s %s",
									  TEST_DB_NAME, metadataFile.getPath(), tableName));

				// copy data files to local in order to run local mapreduce job
				MRFormatTestUtils.copyDataFilesToLocal(metadataFile);

				// transform metadata file to use local file locations
				MRFormatTestUtils.transformMetadata(metadataFile);

			} catch (Exception e) {
				// clean up if any error happens
				FileUtils.deleteDirectory(caseFolder);
				throw e;

			} finally {
				HAWQJdbcUtils.closeConnection(conn);
			}
		}

		// run input format driver
		int exitCode = MRFormatTestUtils.runMapReduceLocally(
				new Path(metadataFile.getPath()),
				new Path(outputFile.getParent()), mapperClass);
		Assert.assertEquals(0, exitCode);

		// compare result
		List<String> outputs = Files.readLines(outputFile, Charsets.UTF_8);
		checkOutput(answers, outputs, table);
		
		System.out.println("Successfully finish test case: " + caseName);
	}
}
