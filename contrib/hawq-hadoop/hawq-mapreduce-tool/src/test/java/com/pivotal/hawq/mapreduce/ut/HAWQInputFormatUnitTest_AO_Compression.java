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
import com.pivotal.hawq.mapreduce.RandomDataProvider;
import com.pivotal.hawq.mapreduce.SimpleTableLocalTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.Test;

import java.util.List;

/**
 * Test reading AO compressed table.
 */
public class HAWQInputFormatUnitTest_AO_Compression extends SimpleTableLocalTester {

	private List<String> colTypes = Lists.newArrayList("int4", "text");
	private DataProvider provider = new RandomDataProvider(500);

	@Test
	public void testZlib() throws Exception {
		for (int compressLevel = 1; compressLevel < 10; compressLevel++) {
			HAWQTable table = new HAWQTable.Builder("test_ao_zlib_" + compressLevel, colTypes)
					.storage(HAWQTableFormat.AO)
					.compress("zlib", compressLevel)
					.provider(provider)
					.build();

			testSimpleTable(table);
		}
	}
}
