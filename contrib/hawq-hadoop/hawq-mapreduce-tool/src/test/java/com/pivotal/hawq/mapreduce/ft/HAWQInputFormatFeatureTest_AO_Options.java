package com.pivotal.hawq.mapreduce.ft;

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
import com.pivotal.hawq.mapreduce.HAWQTable;
import com.pivotal.hawq.mapreduce.SeriesIntDataProvider;
import com.pivotal.hawq.mapreduce.SimpleTableClusterTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * Test reading AO tables with different options.
 */
public class HAWQInputFormatFeatureTest_AO_Options extends SimpleTableClusterTester {

	@BeforeClass
	public static void setUpBeforeClass() {
		System.out.println("Executing test suite: AO_Options");
	}

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
