package com.pivotal.hawq.mapreduce.ut;

import com.google.common.collect.Lists;
import com.pivotal.hawq.mapreduce.HAWQTable;
import com.pivotal.hawq.mapreduce.SeriesIntDataProvider;
import com.pivotal.hawq.mapreduce.SimpleTableLocalTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.Test;

import java.util.List;

/**
 * Test reading AO tables with different options.
 */
public class HAWQInputFormatUnitTest_AO_Options extends SimpleTableLocalTester {

	@Test
	public void testBlockSize() throws Exception {
		List<String> colTypes = Lists.newArrayList("int8");

		int[] blockSizes = {8192, 16384, 65536};
		for (int blockSize : blockSizes) {
			String tableName = "test_ao_blocksize_" + blockSize;
			HAWQTable table = new HAWQTable.Builder(tableName, colTypes)
					.storage(HAWQTableFormat.AO)
					.blockSize(blockSize)
					.provider(new SeriesIntDataProvider(blockSize))
					.build();

			testSimpleTable(table);
		}
	}

	@Test
	public void testChecksum() throws Exception {
		List<String> colTypes = Lists.newArrayList("int4", "varchar(10)");

		HAWQTable table = new HAWQTable.Builder("test_ao_with_checksum", colTypes)
				.storage(HAWQTableFormat.AO)
				.checkSum(true)
				.build();
		testSimpleTable(table);

		table = new HAWQTable.Builder("test_ao_without_checksum", colTypes)
				.storage(HAWQTableFormat.AO)
				.checkSum(false)
				.build();
		testSimpleTable(table);
	}
}
