package com.pivotal.hawq.mapreduce.ft;

import com.google.common.collect.Lists;
import com.pivotal.hawq.mapreduce.DataProvider;
import com.pivotal.hawq.mapreduce.HAWQTable;
import com.pivotal.hawq.mapreduce.SimpleTableClusterTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.Test;

/**
 * Miscellaneous tests for Parquet tables.
 */
public class HAWQInputFormatFeatureTest_Parquet_Misc extends SimpleTableClusterTester {

	@Test
	public void testParquetEmptyTable() throws Exception {
		HAWQTable table = new HAWQTable.Builder("test_parquet_empty", Lists.newArrayList("int4"))
				.storage(HAWQTableFormat.Parquet)
				.provider(DataProvider.EMPTY)
				.build();

		testSimpleTable(table);
	}

	@Test
	public void testParquetRecordGetAllTypes() throws Exception {
		HAWQTable table = new HAWQTable.Builder("test_parquet_alltypes", FeatureTestAllTypesMapper.types)
				.storage(HAWQTableFormat.Parquet)
				.provider(DataProvider.RANDOM)
				.build();

		testSimpleTable(table, FeatureTestAllTypesMapper.class);
	}
}
