package com.pivotal.hawq.mapreduce.ut;

import com.google.common.collect.Lists;
import com.pivotal.hawq.mapreduce.HAWQTable;
import com.pivotal.hawq.mapreduce.SeriesIntDataProvider;
import com.pivotal.hawq.mapreduce.SimpleTableLocalTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.Test;

import java.util.List;

/**
 * Test reading Parquet tables with different options.
 */
public class HAWQInputFormatUnitTest_Parquet_Options extends SimpleTableLocalTester {
	List<String> colTypes = Lists.newArrayList("int8");

	@Test
	public void testPageSize() throws Exception {
		int[] pageSizes = {262144, 524288}; // 256K 512K

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
		int[] rowGroupSizes = {1048576, 2097152};  // 1M 2M

		for (int rowGroupSize : rowGroupSizes) {
			String tableName = "test_parquet_rowgroupsize_" + rowGroupSize;
			HAWQTable table = new HAWQTable.Builder(tableName, colTypes)
					.storage(HAWQTableFormat.Parquet)
					.rowGroupSize(rowGroupSize)
					.pageSize(rowGroupSize / 2)
					.provider(new SeriesIntDataProvider(rowGroupSize / 2))
					.build();

			testSimpleTable(table);
		}
	}
}
