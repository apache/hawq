package com.pivotal.hawq.mapreduce.parquet.convert;

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


import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQGroupField;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;

/**
 * Convert HAWQRecord to Parquet Structure, writes to RecordConsumer
 * User: gaod1
 * Date: 9/10/13
 */
public class HAWQRecordWriter {

	private RecordConsumer consumer;
	private HAWQSchema schema;

	public HAWQRecordWriter(RecordConsumer consumer, HAWQSchema schema) {
		this.consumer = consumer;
		this.schema = schema;
	}

	public void writeRecord(HAWQRecord record) throws HAWQException {
		consumer.startMessage();
		for (int i = 1; i <= schema.getFieldCount(); i++) {
			writeField(schema.getField(i), i, record.getObject(i));
		}
		consumer.endMessage();
	}

	private void writeField(HAWQField fieldSchema, int fieldIndex, Object value) throws HAWQException {
		if (value == null) {
			if (fieldSchema.isOptional()) return;
			else throw new HAWQException("missing value for required field " + fieldSchema.getName());
		}

		consumer.startField(fieldSchema.getName(), fieldIndex - 1);
		if (fieldSchema.isPrimitive()) {
			writePrimitive(fieldSchema.asPrimitive(), value);
		} else {
			writeGroup(fieldSchema.asGroup(), (HAWQRecord) value);
		}
		consumer.endField(fieldSchema.getName(), fieldIndex - 1);
	}

	private void writeGroup(HAWQGroupField groupFieldSchema, HAWQRecord value) throws HAWQException {
		consumer.startGroup();
		for (int i = 1; i <= groupFieldSchema.getFieldCount(); i++) {
			writeField(groupFieldSchema.getField(i), i, value.getObject(i));
		}
		consumer.endGroup();
	}

	private void writePrimitive(HAWQPrimitiveField primitiveFieldSchema, Object value) {
		// TODO
		switch (primitiveFieldSchema.getType()) {
			case BOOL:
				consumer.addBoolean((Boolean) value);
				break;
			case BYTEA:
				consumer.addBinary(Binary.fromByteArray((byte[]) value));
				break;
			case INT2:
				consumer.addInteger(((Short) value).intValue());
				break;
			case INT4:
				consumer.addInteger((Integer) value);
				break;
			case INT8:
				consumer.addLong((Long) value);
				break;
			case FLOAT4:
				consumer.addFloat((Float) value);
				break;
			case FLOAT8:
				consumer.addDouble((Double) value);
				break;
			case VARCHAR:case TEXT:
				consumer.addBinary(Binary.fromString((String) value));
				break;
			case DATE:
				break;
			case TIME:
				break;
			default:
				throw new RuntimeException("unsupported type in HAWQRecordWriter");
		}
	}
}
