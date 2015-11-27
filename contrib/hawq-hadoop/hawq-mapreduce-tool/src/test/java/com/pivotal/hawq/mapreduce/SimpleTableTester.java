package com.pivotal.hawq.mapreduce;

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


import junit.framework.Assert;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Iterator;
import java.util.List;

abstract class SimpleTableTester {

	abstract protected void testSimpleTable(
			HAWQTable table, Class<? extends Mapper> mapperClass) throws Exception;

	protected void testSimpleTable(HAWQTable table) throws Exception {
		testSimpleTable(table, null);
	}

	protected void checkOutput(List<String> answers, List<String> outputs, HAWQTable table) {
		List<String> colTypes = table.getColumnTypes();
		boolean isFloatValue = colTypes.contains("float4") || colTypes.contains("float8");
		if (isFloatValue) {
			Assert.assertTrue("please use float4/float8 only in single column table", colTypes.size() == 1);
		}

		Assert.assertEquals("row number differs!", answers.size(), outputs.size());

		Iterator<String> it1 = answers.iterator();
		Iterator<String> it2 = outputs.iterator();

		while (it1.hasNext() && it2.hasNext()) {
			checkSameResult(it1.next(), it2.next(), isFloatValue);
		}
	}

	private void checkSameResult(String expect, String actual, boolean isFloatValue) {
		if (expect.equals(actual))
			return;

		if (isFloatValue &&
				!actual.equals("null") &&
				!expect.equals("null") &&
				Math.abs(Double.valueOf(expect) - Double.valueOf(actual)) < 1e-5)
			return;

		Assert.fail(String.format("expect row '%s', actual row '%s'",
								  expect, actual));
	}
}
