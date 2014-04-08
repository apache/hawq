package com.pivotal.hawq.mapreduce.ft;

import com.pivotal.hawq.mapreduce.TPCHClusterTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.Test;

/**
 * Test reading TPC-H table using HAWQInputFormat
 */
public class HAWQInputFormatFeatureTest_TPCH extends TPCHClusterTester {

	@Test
	public void testTPCH_AO_No_Partition() throws Exception {
		HAWQTPCHSpec tpchSpec = new HAWQTPCHSpec("0.02", HAWQTableFormat.AO, false);
		testTPCHTable(tpchSpec, "lineitem_ao_row");
	}

	@Test
	public void testTPCH_AO_Partition() throws Exception {
		HAWQTPCHSpec tpchSpec = new HAWQTPCHSpec("0.01", HAWQTableFormat.AO, true);
		testTPCHTable(tpchSpec, "lineitem_ao_row");
	}

	@Test
	public void testTPCH_Parquet_No_Partition() throws Exception {
		HAWQTPCHSpec tpchSpec = new HAWQTPCHSpec("0.02", HAWQTableFormat.Parquet, false);
		testTPCHTable(tpchSpec, "lineitem_ao_parquet");
	}

	@Test
	public void testTPCH_Parquet_Partition() throws Exception {
		HAWQTPCHSpec tpchSpec = new HAWQTPCHSpec("0.01", HAWQTableFormat.Parquet, true);
		testTPCHTable(tpchSpec, "lineitem_ao_parquet");
	}
}
