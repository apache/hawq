package com.pivotal.hawq.mapreduce.parquet.util;

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


import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

import java.util.ArrayList;
import java.util.List;

/**
 * User: gaod1
 * Date: 9/10/13
 */
public final class HAWQSchemaConverter {

	/**
	 * Convert from HAWQ schema to Parquet schema.
	 *
	 * @param hawqSchema schema to be converted.
	 * @return corresponding Parquet schema.
	 */
	public static MessageType convertToParquet(HAWQSchema hawqSchema) {
		List<Type> parquetFields = new ArrayList<Type>();
		for (HAWQField hawqField : hawqSchema.getFields()) {
			parquetFields.add(convertField(hawqField.asPrimitive()));
		}
		return new MessageType(hawqSchema.getName(), parquetFields);
	}

	private static Type convertField(HAWQPrimitiveField hawqField) {
		// FIXME do not consider UDT
		String name = hawqField.getName();
		Repetition repetition = getRepetition(hawqField);
		switch (hawqField.getType()) {
			case BOOL:
				return new PrimitiveType(repetition, PrimitiveTypeName.BOOLEAN, name);
			case BYTEA:
				return new PrimitiveType(repetition, PrimitiveTypeName.BINARY, name);
			case INT2:case INT4:
				return new PrimitiveType(repetition, PrimitiveTypeName.INT32, name);
			case INT8:
				return new PrimitiveType(repetition, PrimitiveTypeName.INT64, name);
			case FLOAT4:
				return new PrimitiveType(repetition, PrimitiveTypeName.FLOAT, name);
			case FLOAT8:
				return new PrimitiveType(repetition, PrimitiveTypeName.DOUBLE, name);
			case VARCHAR:
				return new PrimitiveType(repetition, PrimitiveTypeName.BINARY, name, OriginalType.UTF8);
			/* time-related type */
			case DATE:
				return new PrimitiveType(repetition, PrimitiveTypeName.INT32, name);
			case TIME:
				return new PrimitiveType(repetition, PrimitiveTypeName.INT64, name);
			case TIMETZ:
				return new PrimitiveType(repetition, PrimitiveTypeName.BINARY, name);
			case TIMESTAMP:
				return new PrimitiveType(repetition, PrimitiveTypeName.INT64, name);
			case TIMESTAMPTZ:
				return new PrimitiveType(repetition, PrimitiveTypeName.INT64, name);
			case INTERVAL:
				return new PrimitiveType(repetition, PrimitiveTypeName.BINARY, name);
			default:
				throw new RuntimeException("unsupported hawq type: " + hawqField.getType().name());
		}
	}

	private static Repetition getRepetition(HAWQField field) {
		return field.isOptional() ? Repetition.OPTIONAL : Repetition.REQUIRED;
	}
}
