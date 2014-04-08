package com.pivotal.hawq.mapreduce;

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
				conn = getTestDBConnection();

				// load TPCH data
				Map<String, String> rs = HAWQJdbcUtils.executeSafeQueryForSingleRow(
						conn, "SELECT COUNT(*) segnum FROM gp_segment_configuration WHERE content>=0;");
				int segnum = Integer.parseInt(rs.get("segnum"));
				runShellCommand(tpchSpec.getLoadCmd(segnum));

				// generate answer
				answers = dumpTable(conn, tableName);
				Collections.sort(answers);
				Files.write(Joiner.on('\n').join(answers).getBytes(), answerFile);

				// extract metadata
				runShellCommand(String.format("gpextract -d %s -o %s %s",
											  TEST_DB_NAME, metadataFile.getPath(), tableName));

				// copy data files to local
				copyDataFilesToLocal(metadataFile);

				// transform metadata file to use local file locations
				transformMetadata(metadataFile);

			} catch (Exception e) {
				// clean up if any error happens
				FileUtils.deleteDirectory(caseFolder);
				throw e;

			} finally {
				HAWQJdbcUtils.closeConnection(conn);
			}
		}

		// run input format driver
		int exitCode = runMapReduceLocally(new Path(metadataFile.getPath()),
										   new Path(outputFile.getParent()), null);
		Assert.assertEquals(0, exitCode);

		// compare result
		List<String> outputs = Files.readLines(outputFile, Charsets.UTF_8);

		if (!answers.equals(outputs))
			Assert.fail(String.format("HAWQInputFormat output for table %s differs with DB output:\n%s\n%s",
									  tableName, answerFile, outputFile));
	}
}
