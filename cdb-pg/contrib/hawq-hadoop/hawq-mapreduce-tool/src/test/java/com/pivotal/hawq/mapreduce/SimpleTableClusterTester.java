package com.pivotal.hawq.mapreduce;

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

	@Override
	protected void testSimpleTable(HAWQTable table, Class<? extends Mapper> mapperClass) throws Exception {
		final String tableName = table.getTableName();
		// use table name as case name
		System.out.println("Executing test case: " + tableName);

		final File caseFolder	= new File(FT_TEST_FOLDER, tableName);
		final File sqlFile		= new File(caseFolder, tableName + ".sql");
		final File answerFile	= new File(caseFolder, tableName + ".ans");
		final File outputFile	= new File(caseFolder, tableName + ".out");

		if (caseFolder.exists()) {
			FileUtils.deleteDirectory(caseFolder);
		}
		caseFolder.mkdir();

		Connection conn = null;
		List<String> answers;
		try {
			conn = getTestDBConnection();

			// 1. prepare test data
			String setupSQLs = table.generateDDL() + table.generateData();
			Files.write(setupSQLs.getBytes(), sqlFile);
			runSQLs(conn, setupSQLs);

			// 2. generate answer
			answers = dumpTable(conn, tableName);
			Collections.sort(answers);
			Files.write(Joiner.on('\n').join(answers).getBytes(), answerFile);

		} finally {
			HAWQJdbcUtils.closeConnection(conn);
		}

		// 3. run input format driver
		final Path hdfsOutput = new Path("/temp/hawqinputformat/part-r-00000");
		int exitCode = runMapReduceOnCluster(tableName, hdfsOutput.getParent(), mapperClass);
		Assert.assertEquals(0, exitCode);

		// 4. copy hdfs output to local
		runShellCommand(String.format("hadoop fs -copyToLocal %s %s",
									  hdfsOutput.toString(), outputFile.getPath()));

		// 5. compare result
		List<String> outputs = Files.readLines(outputFile, Charsets.UTF_8);
		checkOutput(answers, outputs, table);

		System.out.println("Successfully finish test case: " + tableName);
	}
}
