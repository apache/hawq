package com.pivotal.hawq.mapreduce.ao.io;

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


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;

import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.junit.*;

import com.pivotal.hawq.mapreduce.HAWQException;

public class HAWQAORecordTest
{

	@Test
	public void TestGetColumnCount()
	{
		HAWQSchema schema = new HAWQSchema("aaa", HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.INT4, "a"));
		String encoding = new String("UTF8");
		HAWQAORecord record;
		try
		{
			record = new HAWQAORecord(schema, encoding, "on i386-apple-darwin");
			record.getSchema().getFieldCount();
		}
		catch (HAWQException e)
		{
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception
	{
		ArrayList<HAWQField> fields = new ArrayList<HAWQField>();
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.INT2, "a"));
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.FLOAT4, "b"));
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.NUMERIC, "c"));
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.VARCHAR, "d"));
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.BOOL, "e"));
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.DATE, "f"));
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.INT8, "g"));
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.TIME, "h"));
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.TIMESTAMP, "i"));
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.FLOAT8, "j"));
		fields.add(HAWQSchema.optional_field(
				HAWQPrimitiveField.PrimitiveType.BYTEA, "l"));
		HAWQAORecord record = new HAWQAORecord(
				new HAWQSchema("alltype", fields), "UTF8", "on i386-apple-darwin");

		record.setShort(1, (short) 1);
		record.setFloat(2, (float) 2.3);
		record.setBigDecimal(3, new BigDecimal("3452345.234234"));
		record.setString(4, "sdfgsgrea");
		record.setBoolean(5, true);
		record.setDate(6, new Date(1323434234355555449l));
		record.setLong(7, 4);
		record.setTime(8, new Time(1323434234355555449l));
		record.setTimestamp(9, new Timestamp(1323434234355555449l));
		record.setDouble(10, 5.6);
		String temp = "dfgsfgs";
		record.setBytes(11, temp.getBytes());

		BufferedWriter bw = new BufferedWriter(new FileWriter(
				"HAWQAORecordTest"));
		long begin = System.currentTimeMillis();
		for (int i = 0; i < 8388608; i++)
		{
			int columnCount = record.getSchema().getFieldCount();
			bw.write(record.getString(1));
			for (int j = 2; j <= columnCount; j++)
			{
				bw.write("|");
				bw.write(record.getString(j));
			}
			bw.write("\n");
		}
		long end = System.currentTimeMillis();
		System.out.println("Time elapsed: " + (end - begin));
		bw.close();
	}
}
