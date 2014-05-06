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

import static com.pivotal.hawq.mapreduce.MRFormatConfiguration.FT_TEST_FOLDER;

/**
 * Test TPCH table using cluster job runner, suitable for Integration Test.
 */
public class TPCHClusterTester extends TPCHTester {

	@Override
	protected void testTPCHTable(HAWQTPCHSpec tpchSpec, String tableName)
			throws Exception {

		String caseName = tpchSpec.toString();
		System.out.println("Executing test case: " + caseName);

		final File caseFolder	= new File(FT_TEST_FOLDER, caseName);
		final File answerFile	= new File(caseFolder, tableName + ".ans");
		final File outputFile	= new File(caseFolder, tableName + ".out");

		if (caseFolder.exists()) {
			FileUtils.deleteDirectory(caseFolder);
		}
		caseFolder.mkdir();

		Connection conn = null;
		List<String> answers;
		try {
			conn = MRFormatTestUtils.getTestDBConnection();

			// 1. load TPCH data
			Map<String, String> rs = HAWQJdbcUtils.executeSafeQueryForSingleRow(
					conn, "SELECT COUNT(*) segnum FROM gp_segment_configuration WHERE content>=0;");
			int segnum = Integer.parseInt(rs.get("segnum"));
			MRFormatTestUtils.runShellCommand(tpchSpec.getLoadCmd(segnum));

			// 2. generate answer
			answers = MRFormatTestUtils.dumpTable(conn, tableName);
			Collections.sort(answers);
			Files.write(Joiner.on('\n').join(answers).getBytes(), answerFile);

		} finally {
			HAWQJdbcUtils.closeConnection(conn);
		}

		// 3. run input format driver
		final Path hdfsOutput	= new Path("/temp/hawqinputformat/part-r-00000");
		int exitCode = MRFormatTestUtils.runMapReduceOnCluster(tableName, hdfsOutput.getParent(), null);
		Assert.assertEquals(0, exitCode);

		// 4. copy hdfs output to local
		MRFormatTestUtils.runShellCommand(
				String.format("hadoop fs -copyToLocal %s %s",
							  hdfsOutput.toString(), outputFile.getPath()));

		// 5. compare result
		List<String> outputs = Files.readLines(outputFile, Charsets.UTF_8);

		if (!answers.equals(outputs))
			Assert.fail(String.format("HAWQInputFormat output for table %s differs with DB output:\n%s\n%s",
									  tableName, answerFile, outputFile));

		System.out.println("Successfully finish test case: " + caseName);
	}
}
