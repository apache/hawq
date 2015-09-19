package com.pivotal.hawq.mapreduce.ft;

import com.google.common.collect.Lists;
import com.pivotal.hawq.mapreduce.DataProvider;
import com.pivotal.hawq.mapreduce.HAWQTable;
import com.pivotal.hawq.mapreduce.SimpleTableClusterTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Miscellaneous tests for AO tables.
 */
public class HAWQInputFormatFeatureTest_AO_Misc extends SimpleTableClusterTester {

	@BeforeClass
	public static void setUpBeforeClass() {
		System.out.println("Executing test suite: AO_Misc");
	}

	@Test
	public void testAOLargeContent() throws Exception {
		HAWQTable table = new HAWQTable.Builder("test_ao_largecontent", Lists.newArrayList("text"))
				.storage(HAWQTableFormat.AO)
				.blockSize(32768)    // 32K
				.provider(new DataProvider() {
					@Override
					public String getInsertSQLs(HAWQTable table) {
						return "INSERT INTO " + table.getTableName() + " values (repeat('b', 40000));";
					}
				}).build();

		testSimpleTable(table);
	}

	@Test
	public void testAOEmptyTable() throws Exception {
		HAWQTable table = new HAWQTable.Builder("test_ao_empty", Lists.newArrayList("int4"))
				.storage(HAWQTableFormat.AO)
				.provider(DataProvider.EMPTY)
				.build();

		testSimpleTable(table);
	}

	@Test
	public void testAORecordGetAllTypes() throws Exception {
		HAWQTable table = new HAWQTable.Builder("test_ao_alltypes", FeatureTestAllTypesMapper.types)
				.storage(HAWQTableFormat.AO)
				.provider(DataProvider.RANDOM)
				.build();

		testSimpleTable(table, FeatureTestAllTypesMapper.class);
	}
}
