package com.pivotal.hawq.mapreduce.ft;

import com.google.common.collect.Lists;
import com.pivotal.hawq.mapreduce.DataProvider;
import com.pivotal.hawq.mapreduce.HAWQTable;
import com.pivotal.hawq.mapreduce.RandomDataProvider;
import com.pivotal.hawq.mapreduce.SimpleTableClusterTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.Test;

import java.util.List;

/**
 * Test reading Parquet compressed table.
 */
public class HAWQInputFormatFeatureTest_Parquet_Compression extends SimpleTableClusterTester {

	private List<String> colTypes = Lists.newArrayList("int4", "text");
	private DataProvider provider = new RandomDataProvider(500);

	@Test
	public void testSnappy() throws Exception {
		HAWQTable table = new HAWQTable.Builder("test_parquet_snappy", colTypes)
				.storage(HAWQTableFormat.Parquet)
				.compress("snappy", 0)
				.provider(provider)
				.build();

		testSimpleTable(table);
	}

	@Test
	public void testGzip() throws Exception {
		for (int compressLevel = 1; compressLevel < 10; compressLevel++) {
			HAWQTable table = new HAWQTable.Builder("test_parquet_gzip_" + compressLevel, colTypes)
					.storage(HAWQTableFormat.Parquet)
					.compress("gzip", compressLevel)
					.provider(provider)
					.build();

			testSimpleTable(table);
		}
	}
}
