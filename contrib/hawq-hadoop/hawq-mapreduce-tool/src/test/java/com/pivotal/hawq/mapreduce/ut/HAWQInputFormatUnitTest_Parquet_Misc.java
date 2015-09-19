package com.pivotal.hawq.mapreduce.ut;

import com.google.common.collect.Lists;
import com.pivotal.hawq.mapreduce.DataProvider;
import com.pivotal.hawq.mapreduce.HAWQTable;
import com.pivotal.hawq.mapreduce.SimpleTableLocalTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.Test;

/**
 * Miscellaneous tests for Parquet tables.
 */
public class HAWQInputFormatUnitTest_Parquet_Misc extends SimpleTableLocalTester {

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
		HAWQTable table = new HAWQTable.Builder("test_parquet_alltypes", UnitTestAllTypesMapper.types)
				.storage(HAWQTableFormat.Parquet)
				.provider(DataProvider.RANDOM)
				.build();

		testSimpleTable(table, UnitTestAllTypesMapper.class);
	}
}
