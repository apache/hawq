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


import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Mapper class used in test_ao_alltypes and test_parquet_alltypes UT.
 */
class UnitTestAllTypesMapper extends Mapper<Void, HAWQRecord, Text, Text> {
	private String fieldToString(Object val) throws HAWQException {
		if (val == null) return "null";
		if (val instanceof byte[]) return new String((byte[]) val);
		return val.toString();
	}

	static final String SEPARATOR = "|";

	// Due to floating point values are tricker to test equality, we only allow float4/float8
	// in single column test table. Therefore we don't include float4/float8 in all type tests.
	// timetz/timestamptz is also excluded due to timezone issue.
	static final List<String> types = Lists.newArrayList(
			"bool", "bit", "varbit", "bytea", "int2", "int4", "int8", /*"float4", "float8",*/ "numeric",
			"char(10)", "varchar(10)", "text", "date", "time", /*"timetz",*/ "timestamp", /*"timestamptz",*/ "interval",
			"point", "lseg", "box", "circle", "path", "polygon", "macaddr", "inet", "cidr", "xml");

	@Override
	protected void map(Void key, HAWQRecord value, Context context)
			throws IOException, InterruptedException {
		try {
			List<String> values = Lists.newArrayList();

			values.add(fieldToString(value.getBoolean(1)));
			values.add(fieldToString(value.getByte(2)));
			values.add(fieldToString(value.getVarbit(3)));
			values.add(fieldToString(value.getBytes(4)));
			values.add(fieldToString(value.getShort(5)));
			values.add(fieldToString(value.getInt(6)));
			values.add(fieldToString(value.getLong(7)));
			values.add(fieldToString(value.getBigDecimal(8)));
			values.add(fieldToString(value.getString(9)));
			values.add(fieldToString(value.getString(10)));
			values.add(fieldToString(value.getString(11)));
			values.add(fieldToString(value.getDate(12)));
			values.add(fieldToString(value.getTime(13)));
			values.add(fieldToString(value.getTimestamp(14)));
			values.add(fieldToString(value.getInterval(15)));
			values.add(fieldToString(value.getPoint(16)));
			values.add(fieldToString(value.getLseg(17)));
			values.add(fieldToString(value.getBox(18)));
			values.add(fieldToString(value.getCircle(19)));
			values.add(fieldToString(value.getPath(20)));
			values.add(fieldToString(value.getPolygon(21)));
			values.add(fieldToString(value.getMacaddr(22)));
			values.add(fieldToString(value.getInet(23)));
			values.add(fieldToString(value.getCidr(24)));
			values.add(fieldToString(value.getString(25)));

			// some type's get method will return a default value instead of null,
			// we must take care of it!
			for (int i  = 1; i <= value.getSchema().getFieldCount(); i++) {
				if (value.isNull(i)) {
					values.set(i - 1, "null");
				}
			}

			Text text = new Text(Joiner.on(SEPARATOR).join(values));
			context.write(text, text);

		} catch (HAWQException e) {
			throw new IOException(e);
		}
	}
}
