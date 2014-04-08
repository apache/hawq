package com.pivotal.hawq.mapreduce.ft;

import com.google.common.collect.Lists;
import com.pivotal.hawq.mapreduce.HAWQTable;
import com.pivotal.hawq.mapreduce.SeriesIntDataProvider;
import com.pivotal.hawq.mapreduce.SimpleTableClusterTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.Test;

import java.util.List;

/**
 * Test reading Parquet tables with different options.
 */
public class HAWQInputFormatFeatureTest_Parquet_Options extends SimpleTableClusterTester {
	List<String> colTypes = Lists.newArrayList("int8");

	@Test
	public void testPageSize() throws Exception {
		int[] pageSizes = {1048576, 2097152, 4194304}; // 1M 2M 4M

		for (int pageSize : pageSizes) {
			String tableName = "test_parquet_pagesize_" + pageSize;
			HAWQTable table = new HAWQTable.Builder(tableName, colTypes)
					.storage(HAWQTableFormat.Parquet)
					.pageSize(pageSize)
					.provider(new SeriesIntDataProvider(pageSize))
					.build();

			testSimpleTable(table);
		}
	}

	@Test
	public void testRowGroupSize() throws Exception {
		int[] rowGroupSizes = {8388608, 9437184, 10485760};	// 8M 9M 10M

		for (int rowGroupSize : rowGroupSizes) {
			String tableName = "test_parquet_rowgroupsize_" + rowGroupSize;
			HAWQTable table = new HAWQTable.Builder(tableName, colTypes)
					.storage(HAWQTableFormat.Parquet)
					.rowGroupSize(rowGroupSize)
					.provider(new SeriesIntDataProvider(rowGroupSize / 2))
					.build();

			testSimpleTable(table);
		}
	}
}
