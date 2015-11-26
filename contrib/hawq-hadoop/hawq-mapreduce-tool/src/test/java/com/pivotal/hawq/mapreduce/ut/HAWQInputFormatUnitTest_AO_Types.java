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
import com.pivotal.hawq.mapreduce.HAWQTable;
import com.pivotal.hawq.mapreduce.SimpleTableLocalTester;
import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test data type support for AO table.
 */
public class HAWQInputFormatUnitTest_AO_Types extends SimpleTableLocalTester {

	private void doSingleColumnAOTest(String columnType) throws Exception {
		String tableName = "test_ao_types_" + columnType;
		tableName = tableName.replace("(", "")
							 .replace(")", "")
							 .replace("[]", "_array");

		HAWQTable table = new HAWQTable.Builder(tableName, Lists.newArrayList(columnType))
				.storage(HAWQTableFormat.AO)
				.build();

		testSimpleTable(table);
	}

	@Test
	public void testBool() throws Exception {
		doSingleColumnAOTest("bool");
	}

	@Test
	public void testBit() throws Exception {
		doSingleColumnAOTest("bit");
	}

	@Test
	public void testBitN() throws Exception {
		doSingleColumnAOTest("bit(5)");
	}

	@Test
	public void testVarbit() throws Exception {
		doSingleColumnAOTest("varbit");
	}

	@Test
	public void testByteArray() throws Exception {
		doSingleColumnAOTest("bytea");
	}

	@Test
	public void testInt2() throws Exception {
		doSingleColumnAOTest("int2");
	}

	@Test
	public void testInt4() throws Exception {
		doSingleColumnAOTest("int4");
	}

	@Test
	public void testInt8() throws Exception {
		doSingleColumnAOTest("int8");
	}

	@Test
	public void testFloat4() throws Exception {
		doSingleColumnAOTest("float4");
	}

	@Test
	public void testFloat8() throws Exception {
		doSingleColumnAOTest("float8");
	}

	@Test
	public void testNumeric() throws Exception {
		doSingleColumnAOTest("numeric");
	}

	@Test
	public void testCharN() throws Exception {
		doSingleColumnAOTest("char(10)");
	}

	@Test
	public void testVarcharN() throws Exception {
		doSingleColumnAOTest("varchar(10)");
	}

	@Test
	public void testText() throws Exception {
		doSingleColumnAOTest("text");
	}

	@Test
	public void testDate() throws Exception {
		doSingleColumnAOTest("date");
	}

	@Test
	public void testTime() throws Exception {
		doSingleColumnAOTest("time");
	}

	@Ignore("we cannot use a static answer file for timetz as we did in all " +
			"UTs, because its value depends on the machine running the tests." +
			"Nonetheless, timetz will still be tested in FT")
	@Test
	public void testTimetz() throws Exception {
		doSingleColumnAOTest("timetz");
	}

	@Test
	public void testTimestamp() throws Exception {
		doSingleColumnAOTest("timestamp");
	}

	@Ignore("the same reason as timetz")
	@Test
	public void testTimestamptz() throws Exception {
		doSingleColumnAOTest("timestamptz");
	}

	@Test
	public void testInterval() throws Exception {
		doSingleColumnAOTest("interval");
	}

	@Test
	public void testPoint() throws Exception {
		doSingleColumnAOTest("point");
	}

	@Test
	public void testLseg() throws Exception {
		doSingleColumnAOTest("lseg");
	}

	@Test
	public void testBox() throws Exception {
		doSingleColumnAOTest("box");
	}

	@Test
	public void testCircle() throws Exception {
		doSingleColumnAOTest("circle");
	}

	@Test
	public void testPath() throws Exception {
		doSingleColumnAOTest("path");
	}

	@Test
	public void testPolygon() throws Exception {
		doSingleColumnAOTest("polygon");
	}

	@Test
	public void testMacaddr() throws Exception {
		doSingleColumnAOTest("macaddr");
	}

	@Test
	public void testInet() throws Exception {
		doSingleColumnAOTest("inet");
	}

	@Test
	public void testCidr() throws Exception {
		doSingleColumnAOTest("cidr");
	}

	@Test
	public void testXml() throws Exception {
		doSingleColumnAOTest("xml");
	}

	@Test
	public void testArray() throws Exception {
		doSingleColumnAOTest("int4[]");
	}
}
