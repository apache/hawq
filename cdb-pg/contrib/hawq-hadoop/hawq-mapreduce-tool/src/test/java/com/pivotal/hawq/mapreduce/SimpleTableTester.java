package com.pivotal.hawq.mapreduce;

import junit.framework.Assert;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Iterator;
import java.util.List;

abstract class SimpleTableTester extends MRFormatTester {

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
