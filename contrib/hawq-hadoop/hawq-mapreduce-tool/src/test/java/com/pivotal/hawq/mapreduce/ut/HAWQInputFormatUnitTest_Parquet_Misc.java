package com.pivotal.hawq.mapreduce.ut;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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
