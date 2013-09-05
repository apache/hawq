package com.pivotal.hawq.mapreduce.parquet.convert;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.parquet.HAWQParquetRecord;
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * User: gaod1
 * Date: 8/8/13
 */
public class HAWQRecordConverter extends GroupConverter {

	private final ParentValueContainer parent;
	private HAWQSchema hawqSchema;
	private final Converter[] converters;
	private HAWQParquetRecord currentRecord;

	// FIXME Converter deal with requestedHAWQSchema only? (construct from requestedSchema and hawqSchema)
	public HAWQRecordConverter(MessageType requestedSchema, HAWQSchema hawqSchema) {
		this(null, requestedSchema, hawqSchema);
	}

	public HAWQRecordConverter(ParentValueContainer parent, MessageType requestedSchema, HAWQSchema hawqSchema) {
		this.parent = parent;
		this.hawqSchema = hawqSchema;

		int fieldsNum = hawqSchema.getFieldCount();
		this.converters = new Converter[fieldsNum];

		int fieldIndex = 0;
		for (HAWQField field : hawqSchema.getFields()) {
			final int recordFieldIndex = fieldIndex + 1;  // index in HAWQRecord starts from 1
			Converter fieldConverter = newConverter(field, new ParentValueContainer() {

				@Override
				void setBoolean(boolean x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setBoolean(recordFieldIndex, x);
				}

				@Override
				void setByte(byte x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setByte(recordFieldIndex, x);
				}

				@Override
				void setBytes(byte[] x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setBytes(recordFieldIndex, x);
				}

				@Override
				void setDouble(double x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setDouble(recordFieldIndex, x);
				}

				@Override
				void setFloat(float x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setFloat(recordFieldIndex, x);
				}

				@Override
				void setInt(int x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setInt(recordFieldIndex, x);
				}

				@Override
				void setLong(long x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setLong(recordFieldIndex, x);
				}

				@Override
				void setShort(short x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setShort(recordFieldIndex, x);
				}

				@Override
				void setString(String x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setString(recordFieldIndex, x);
				}

				@Override
				void setNull() throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setNull(recordFieldIndex);
				}

				@Override
				void setTimestamp(Timestamp x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setTimestamp(recordFieldIndex, x);
				}

				@Override
				void setTime(Time x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setTime(recordFieldIndex, x);
				}

				@Override
				void setDate(Date x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setDate(recordFieldIndex, x);
				}

				@Override
				void setBigDecimal(BigDecimal x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setBigDecimal(recordFieldIndex, x);
				}

				@Override
				void setArray(Array x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setArray(recordFieldIndex, x);
				}

				@Override
				void setField(HAWQRecord x) throws HAWQException {
					HAWQRecordConverter.this.currentRecord.setField(recordFieldIndex, x);
				}
			});
			this.converters[fieldIndex++] = fieldConverter;
		}

	}

	private Converter newConverter(HAWQField hawqType, ParentValueContainer parent) {
		/*
		 * !!
		 * 
		 * Dayue should redesign this function
		 */
		if (!hawqType.isPrimitive())
//			return new HAWQRecordConverter(parent, null, hawqType);
			return null;
		switch (hawqType.asPrimitive().getType()) {
			case BOOL:case INT4:case INT8:case FLOAT4:case FLOAT8:
				return new HAWQPrimitiveConverter(parent);
			case INT2:
				return new HAWQShortConverter(parent);
			case NUMERIC:
				return new HAWQBigDecimalConverter(parent);
			case BPCHAR:case VARCHAR:case TEXT:
				return new HAWQStringConverter(parent);
			case BYTEA:
				return new HAWQByteArrayConverter(parent);
			case DATE:
				return new HAWQDateConverter(parent);
			case TIME:
				return new HAWQTimeConverter(parent);
			case TIMESTAMP:
				return new HAWQTimestampConverter(parent);
			case POINT:
				return new HAWQPointConverter(parent);
			case LSEG:
				return new HAWQLineSegmentConverter(parent);
			case BOX:
				return new HAWQBoxConverter(parent);
			case CIRCLE:
				return new HAWQCircleConverter(parent);
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
		currentRecord = new HAWQParquetRecord(hawqSchema);
	}

	@Override
	public void end() {
		if (parent != null) {
			try {
				parent.setField(currentRecord);
			} catch (HAWQException e) {}
		}
	}

	public HAWQParquetRecord getCurrentRecord() {
		return currentRecord;
	}

	static abstract class ParentValueContainer {
		abstract void setBoolean(boolean x) throws HAWQException;

		abstract void setByte(byte x) throws HAWQException;

		abstract void setBytes(byte[] x) throws HAWQException;

		abstract void setDouble(double x) throws HAWQException;

		abstract void setFloat(float x) throws HAWQException;

		abstract void setInt(int x) throws HAWQException;

		abstract void setLong(long x) throws HAWQException;

		abstract void setShort(short x) throws HAWQException;

		abstract void setString(String x) throws HAWQException;

		abstract void setNull() throws HAWQException; // TODO

		abstract void setTimestamp(Timestamp x) throws HAWQException;

		abstract void setTime(Time x) throws HAWQException;

		abstract void setDate(Date x) throws HAWQException;

		abstract void setBigDecimal(BigDecimal x) throws HAWQException;

		abstract void setArray(Array x) throws HAWQException;

		abstract void setField(HAWQRecord x) throws HAWQException;
	}

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

	//TODO
	static class HAWQBigDecimalConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQBigDecimalConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
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
			} catch (HAWQException e) {}
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

	//TODO date is stored in 4 bytes binary
	static class HAWQDateConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQDateConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {

		}
	}

	//TODO time (without timezone) is stored in 8 bytes binary
	static class HAWQTimeConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQTimeConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {

		}
	}

	//TODO timestamp (without timezone) is stored in 8 bytes binary
	static class HAWQTimestampConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQTimestampConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
		}
	}

	//TODO point is stored in 16 bytes binary (x,y)
	static class HAWQPointConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQPointConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
		}
	}

	//TODO line segment is stored in 32 bytes binary (x1,y1,x2,y2)
	static class HAWQLineSegmentConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQLineSegmentConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
		}
	}

	//TODO box is stored in 32 bytes binary (x1,y1,x2,y2)
	static class HAWQBoxConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQBoxConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
		}
	}

	//TODO circle is stored in 24 bytes binary (x,y,r)
	static class HAWQCircleConverter extends PrimitiveConverter {
		private ParentValueContainer parent;

		public HAWQCircleConverter(ParentValueContainer parent) {
			this.parent = parent;
		}

		@Override
		public void addBinary(Binary value) {
		}
	}
}