package com.pivotal.hawq.mapreduce.ut;

import com.pivotal.hawq.mapreduce.TPCHLocalTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.Test;

/**
 * Test reading TPC-H table using HAWQInputFormat
 */
public class HAWQInputFormatUnitTest_TPCH extends TPCHLocalTester {

	@Test
	public void testTPCH_AO_No_Partition() throws Exception {
		HAWQTPCHSpec tpchSpec = new HAWQTPCHSpec("0.001", HAWQTableFormat.AO, false);
		testTPCHTable(tpchSpec, "lineitem_ao_row");
	}

	@Test
	public void testTPCH_AO_Partition() throws Exception {
		HAWQTPCHSpec tpchSpec = new HAWQTPCHSpec("0.001", HAWQTableFormat.AO, true);
		testTPCHTable(tpchSpec, "lineitem_ao_row");
	}

	@Test
	public void testTPCH_Parquet_No_Partition() throws Exception {
		HAWQTPCHSpec tpchSpec = new HAWQTPCHSpec("0.001", HAWQTableFormat.Parquet, false);
		testTPCHTable(tpchSpec, "lineitem_ao_parquet");
	}

	@Test
	public void testTPCH_Parquet_Partition() throws Exception {
		HAWQTPCHSpec tpchSpec = new HAWQTPCHSpec("0.001", HAWQTableFormat.Parquet, true);
		testTPCHTable(tpchSpec, "lineitem_ao_parquet");
	}
}
