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
import com.pivotal.hawq.mapreduce.datatype.*;
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import com.pivotal.hawq.mapreduce.util.HAWQConvertUtil;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.MessageType;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * HAWQ's implementation of Parquet's GroupConverter.
 */
public class HAWQRecordConverter extends GroupConverter {

	private final ParentValueContainer parent;
	private HAWQSchema hawqSchema;
	private final Converter[] converters;
	private HAWQRecord currentRecord;

	// TODO maybe HAWQRecordConverter(HAWQSchema requestedSchema, HAWQSchema hawqSchema) ?
	public HAWQRecordConverter(MessageType requestedSchema, HAWQSchema hawqSchema) {
		this(null, requestedSchema, hawqSchema);
	}

	public HAWQRecordConverter(ParentValueContainer parent, MessageType requestedSchema, HAWQSchema hawqSchema) {
		this.parent = parent;
		this.hawqSchema = hawqSchema;

		int fieldsNum = hawqSchema.getFieldCount();
		this.converters = new Converter[fieldsNum];

		int fieldIndex = 0;  // index of converter starts from 0
		for (HAWQField field : hawqSchema.getFields()) {
			final int recordFieldIndex = fieldIndex + 1;  // index in HAWQRecord starts from 1
			Converter fieldConverter = newConverter(field, new ParentValueContainer() {
				@Override
				public void setBoolean(boolean x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setBoolean(recordFieldIndex, x);
				}

				@Override
				public void setBit(HAWQVarbit x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setBit(recordFieldIndex, x);
				}

				@Override
				public void setByte(byte x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setByte(recordFieldIndex, x);
				}

				@Override
				public void setBytes(byte[] x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setBytes(recordFieldIndex, x);
				}

				@Override
				public void setShort(short x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setShort(recordFieldIndex, x);
				}

				@Override
				public void setInt(int x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setInt(recordFieldIndex, x);
				}

				@Override
				public void setLong(long x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setLong(recordFieldIndex, x);
				}

				@Override
				public void setFloat(float x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setFloat(recordFieldIndex, x);
				}

				@Override
				public void setDouble(double x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setDouble(recordFieldIndex, x);
				}

				@Override
				public void setBigDecimal(BigDecimal x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setBigDecimal(recordFieldIndex, x);
				}

				@Override
				public void setString(String x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setString(recordFieldIndex, x);
				}

				@Override
				public void setDate(Date x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setDate(recordFieldIndex, x);
				}

				@Override
				public void setTime(Time x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setTime(recordFieldIndex, x);
				}

				@Override
				public void setTimestamp(Timestamp x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setTimestamp(recordFieldIndex, x);
				}

				@Override
				public void setInterval(HAWQInterval x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setInterval(recordFieldIndex, x);
				}

				@Override
				public void setPoint(HAWQPoint x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setPoint(recordFieldIndex, x);
				}

				@Override
				public void setLseg(HAWQLseg x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setLseg(recordFieldIndex, x);
				}

				@Override
				public void setBox(HAWQBox x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setBox(recordFieldIndex, x);
				}

				@Override
				public void setCircle(HAWQCircle x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setCircle(recordFieldIndex, x);
				}

				@Override
				public void setPath(HAWQPath x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setPath(recordFieldIndex, x);
				}

				@Override
				public void setPolygon(HAWQPolygon x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setPolygon(recordFieldIndex, x);
				}

				@Override
				public void setMacaddr(HAWQMacaddr x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setMacaddr(recordFieldIndex, x);
				}

				@Override
				public void setInet(HAWQInet x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setInet(recordFieldIndex, x);
				}

				@Override
				public void setCidr(HAWQCidr x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setCidr(recordFieldIndex, x);
				}

				@Override
				public void setArray(Array x) throws HAWQException {
					throw new UnsupportedOperationException();  // TODO
				}

				@Override
				public void setField(HAWQRecord x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setField(recordFieldIndex, x);
				}
			});
			this.converters[fieldIndex++] = fieldConverter;
		}
	}

	private Converter newConverter(HAWQField hawqType, ParentValueContainer parent) {
		if (!hawqType.isPrimitive())  // FIXME
			throw new RuntimeException("HAWQRecordConverter.newConverter not implement group type converter");

		switch (hawqType.asPrimitive().getType()) {
			case BIT:case VARBIT:
				return new HAWQBitsConverter(parent);
			case BYTEA:
				return new HAWQByteArrayConverter(parent);
			/* number related type */
			case BOOL:case INT4:case INT8:case FLOAT4:case FLOAT8:
				return new HAWQPrimitiveConverter(parent);
			case INT2:
				return new HAWQShortConverter(parent);
			case NUMERIC:
				return new HAWQBigDecimalConverter(parent);
			/* string related type */
			case BPCHAR:case VARCHAR:case TEXT:case XML:
				return new HAWQStringConverter(parent);
			/* time related type */
			case DATE:
				return new HAWQDateConverter(parent);
			case TIME:
				return new HAWQTimeConverter(parent);
			case TIMETZ:
				return new HAWQTimeTZConverter(parent);
			case TIMESTAMP:
				return new HAWQTimestampConverter(parent);
			case TIMESTAMPTZ:
				return new HAWQTimestampTZConverter(parent);
			case INTERVAL:
				return new HAWQIntervalConverter(parent);
			/* geometry related type */
			case POINT:
				return new HAWQPointConverter(parent);
			case LSEG:
				return new HAWQLineSegmentConverter(parent);
			case PATH:
				return new HAWQPathConverter(parent);
			case BOX:
				return new HAWQBoxConverter(parent);
			case POLYGON:
				return new HAWQPolygonConverter(parent);
			case CIRCLE:
				return new HAWQCircleConverter(parent);
			/* other type */
			case MACADDR:
				return new HAWQMacaddrConverter(parent);
			case INET:
				return new HAWQInetConverter(parent);
			case CIDR:
				return new HAWQCidrConverter(parent);
			default:
				throw new UnsupportedOperationException("unsupported type " + hawqType);
		}
	}

	@Override
	public Converter getConverter(int fieldIndex) {
		return converters[fieldIndex];
	}

	@Override
	public void start() {
		currentRecord = new HAWQRecord(hawqSchema);
	}

	@Override
	public void end() {
		if (parent != null) {
			try {
				parent.setField(currentRecord);
			} catch (HAWQException e) {}
		}
	}

	public HAWQRecord getCurrentRecord() {
		return currentRecord;
	}

	//////////////////////////////////////////////////////////////
	/// converters from parquet data type to HAWQ data type
	//////////////////////////////////////////////////////////////

	static class HAWQPrimitiveConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQPrimitiveConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBoolean(boolean value) {
			try {
				parent.setBoolean(value);
			} catch (HAWQException e) {}
		}

		@Override
		public void addInt(int value) {
			try {
				parent.setInt(value);
			} catch (HAWQException e) {}
		}

		@Override
		public void addLong(long value) {
			try {
				parent.setLong(value);
			} catch (HAWQException e) {}
		}

		@Override
		public void addFloat(float value) {
			try {
				parent.setFloat(value);
			} catch (HAWQException e) {}
		}

		@Override
		public void addDouble(double value) {
			try {
				parent.setDouble(value);
			} catch (HAWQException e) {}
		}
	}

	static class HAWQShortConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQShortConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addInt(int value) {
			try {
				parent.setShort((short) value);
			} catch (HAWQException e) {}
		}
	}

	static class HAWQBigDecimalConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQBigDecimalConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
			try {
				// FIXME bytesToDecimal return "NAN" case
				parent.setBigDecimal((BigDecimal) HAWQConvertUtil.bytesToDecimal(value.getBytes(), 0));
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Binary to BigDecimal", e);
			}
		}
	}

	static class HAWQStringConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQStringConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
			try {
				parent.setString(value.toStringUsingUTF8());
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Binary to String", e);
			}
		}
	}

	static class HAWQBitsConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQBitsConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
			try {
				parent.setBit(HAWQConvertUtil.bytesToVarbit(value.getBytes(), 0));
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Binary to Varbit");
			}
		}
	}

	static class HAWQByteArrayConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQByteArrayConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
			try {
				parent.setBytes(value.getBytes());
			} catch (HAWQException e) {}
		}
	}

	// date is stored as a 4-bytes int
	static class HAWQDateConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQDateConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addInt(int value) {
			try {
				parent.setDate(HAWQConvertUtil.toDate(value));

			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Integer to Date", e);
			}
		}
	}

	// time (without timezone) is stored in 8-bytes long
	static class HAWQTimeConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQTimeConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addLong(long value) {
			try {
				parent.setTime(HAWQConvertUtil.toTime(value));
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Long to Time", e);
			}
		}
	}

	// time (with timezone) is stored in 12 bytes binary
	static class HAWQTimeTZConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQTimeTZConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
			try {
				parent.setTime(HAWQConvertUtil.toTimeTz(value.getBytes(), 0));
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Binary to Time (with timezone)", e);
			}
		}
	}

	// timestamp (without timezone) is stored in 8-bytes long
	static class HAWQTimestampConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQTimestampConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addLong(long value) {
			try {
				parent.setTimestamp(HAWQConvertUtil.toTimestamp(value, false));
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Long to Timestamp", e);
			}
		}
	}

	// timestamp (with timezone) is stored in 8-bytes long
	static class HAWQTimestampTZConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQTimestampTZConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addLong(long value) {
			try {
				parent.setTimestamp(HAWQConvertUtil.toTimestamp(value, true));
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Long to Timestamp (with timezone)", e);
			}
		}
	}

	// interval is stored in 16 bytes binary
	static class HAWQIntervalConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQIntervalConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
			try {
				parent.setInterval(HAWQConvertUtil.bytesToInterval(value.getBytes(), 0));
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Binary to Interval", e);
			}
		}
	}

	// macaddr is stored as binary
	static class HAWQMacaddrConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQMacaddrConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
			try {
				parent.setMacaddr(new HAWQMacaddr(value.getBytes()));
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Binary to HAWQMacaddr");
			}
		}
	}

	// inet is stored as binary
	static class HAWQInetConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		HAWQInetConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
			try {
				parent.setInet(HAWQConvertUtil.bytesToInet(value.getBytes(), 0));
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Binary to HAWQInet");
			}
		}
	}

	// cidr is stored as binary
	static class HAWQCidrConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		HAWQCidrConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
			try {
				parent.setCidr(HAWQConvertUtil.bytesToCidr(value.getBytes(), 0));
			} catch (HAWQException e) {
				throw new RuntimeException("error during conversion from Binary to HAWQCidr");
			}
		}
	}
}
