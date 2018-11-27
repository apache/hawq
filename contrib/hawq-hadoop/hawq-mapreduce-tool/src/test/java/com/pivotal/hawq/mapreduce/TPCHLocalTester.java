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

import java.io.File;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.pivotal.hawq.mapreduce.MRFormatConfiguration.TEST_DB_NAME;
import static com.pivotal.hawq.mapreduce.MRFormatConfiguration.TEST_FOLDER;

/**
 * Test TPCH table using local job runner, suitable for Unit Test.
 */
public class TPCHLocalTester extends TPCHTester {

	@Override
	protected void testTPCHTable(HAWQTPCHSpec tpchSpec, String tableName)
			throws Exception {
		String caseName = tpchSpec.toString();

		final File caseFolder	= new File(TEST_FOLDER, caseName);
		final File metadataFile	= new File(caseFolder, tableName + ".yaml");
		final File answerFile	= new File(caseFolder, tableName + ".ans");
		final File outputFile	= new File(caseFolder, "output/part-r-00000");

		List<String> answers;

		if (caseFolder.exists()) {
			answers = Files.readLines(answerFile, Charsets.UTF_8);

		} else {
			caseFolder.mkdir();

			Connection conn = null;
			try {
				conn = MRFormatTestUtils.getTestDBConnection();

				// load TPCH data
				Map<String, String> rs = HAWQJdbcUtils.executeSafeQueryForSingleRow(
						conn, "SELECT COUNT(*) segnum FROM gp_segment_configuration WHERE role='p';");
				int segnum = Integer.parseInt(rs.get("segnum"));
				MRFormatTestUtils.runShellCommand(tpchSpec.getLoadCmd(segnum));

				// generate answer
				answers = MRFormatTestUtils.dumpTable(conn, tableName);
				Collections.sort(answers);
				Files.write(Joiner.on('\n').join(answers).getBytes(), answerFile);

				// extract metadata
				MRFormatTestUtils.runShellCommand(
						String.format("hawq extract -d %s -o %s %s",
									  TEST_DB_NAME, metadataFile.getPath(), tableName));

				// copy data files to local
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
				new Path(outputFile.getParent()), null);
		Assert.assertEquals(0, exitCode);

		// compare result
		List<String> outputs = Files.readLines(outputFile, Charsets.UTF_8);

		if (!answers.equals(outputs))
			Assert.fail(String.format("HAWQInputFormat output for table %s differs with DB output:\n%s\n%s",
									  tableName, answerFile, outputFile));
	}
}
