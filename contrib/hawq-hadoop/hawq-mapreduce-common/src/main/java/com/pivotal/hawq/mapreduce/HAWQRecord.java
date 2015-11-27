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


import com.pivotal.hawq.mapreduce.datatype.*;
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Store schema and values for a record in database.
 * <p/>
 * User can use getXXX method in input format.
 */
public class HAWQRecord {

	protected HAWQSchema schema;
	protected Object[] values;

	public HAWQRecord(HAWQSchema schema) {
		this.schema = schema;
		this.values = new Object[schema.getFieldCount()];
	}

	protected void checkFieldIndex(int fieldIndex) throws HAWQException {
		if (fieldIndex < 1 || fieldIndex > values.length)
			throw new HAWQException(String.format("index out of range [%d, %d]", 1, values.length));
	}

	/**
	 * Get schema of this record
	 *
	 * @return the schema in this record
	 */
	public HAWQSchema getSchema() {
		return schema;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * boolean in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>false</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public boolean getBoolean(String fieldName) throws HAWQException {
		return getBoolean(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * boolean in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>false</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public boolean getBoolean(int fieldIndex) throws HAWQException {
		return toBoolean(getString(fieldIndex));
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setBoolean(String fieldName, boolean newvalue)
			throws HAWQException {
		setBoolean(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setBoolean(int fieldIndex, boolean newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * byte in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public byte getByte(String fieldName) throws HAWQException {
		return getByte(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * byte in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public byte getByte(int fieldIndex) throws HAWQException {
		return toByte(getString(fieldIndex));
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setByte(String fieldName, byte newvalue) throws HAWQException {
		setByte(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setByte(int fieldIndex, byte newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * byte[] in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public byte[] getBytes(String fieldName) throws HAWQException {
		return getBytes(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * byte[] in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public byte[] getBytes(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (byte[]) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setBytes(String fieldName, byte[] newvalue)
			throws HAWQException {
		setBytes(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setBytes(int fieldIndex, byte[] newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * double in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public double getDouble(String fieldName) throws HAWQException {
		return getDouble(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * double in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public double getDouble(int fieldIndex) throws HAWQException {
		return toDouble(getString(fieldIndex));
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setDouble(String fieldName, double newvalue)
			throws HAWQException {
		setDouble(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setDouble(int fieldIndex, double newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * float in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public float getFloat(String fieldName) throws HAWQException {
		return getFloat(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * float in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public float getFloat(int fieldIndex) throws HAWQException {
		return toFloat(getString(fieldIndex));
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setFloat(String fieldName, float newvalue) throws HAWQException {
		setFloat(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setFloat(int fieldIndex, float newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * int in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public int getInt(String fieldName) throws HAWQException {
		return getInt(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * int in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public int getInt(int fieldIndex) throws HAWQException {
		return toInt(getString(fieldIndex));
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setInt(String fieldName, int newvalue) throws HAWQException {
		setInt(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setInt(int fieldIndex, int newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * long in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public long getLong(String fieldName) throws HAWQException {
		return getLong(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * long in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public long getLong(int fieldIndex) throws HAWQException {
		return toLong(getString(fieldIndex));
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setLong(String fieldName, long newvalue) throws HAWQException {
		setLong(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setLong(int fieldIndex, long newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * short in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public short getShort(String fieldName) throws HAWQException {
		return getShort(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * short in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public short getShort(int fieldIndex) throws HAWQException {
		return toShort(getString(fieldIndex));
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setShort(String fieldName, short newvalue) throws HAWQException {
		setShort(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setShort(int fieldIndex, short newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * String in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public String getString(String fieldName) throws HAWQException {
		return getString(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * String in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public String getString(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = this.values[fieldIndex - 1];
		return val == null ? null : val.toString();
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setString(String fieldName, String newvalue)
			throws HAWQException {
		setString(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setString(int fieldIndex, String newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * char in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public char getChar(String fieldName) throws HAWQException {
		return getChar(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * char in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>0</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public char getChar(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		HAWQField field = schema.getField(fieldIndex);

		if (!field.isPrimitive())
			throw new HAWQException("cannot use of getChar on group field: " + field.getName());

		HAWQPrimitiveField.PrimitiveType fieldType = field.asPrimitive().getType();
		if (fieldType != HAWQPrimitiveField.PrimitiveType.BPCHAR
			&& fieldType != HAWQPrimitiveField.PrimitiveType.VARCHAR
			&& fieldType != HAWQPrimitiveField.PrimitiveType.TEXT) {
			throw new HAWQException("cannot use of getChar on field type: " + fieldType);
		}

		String s = getString(fieldIndex);
		if (s == null) return 0;
		if (s.length() == 1) return s.charAt(0);
		throw new HAWQException("Bad value for type char : " + s);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setChar(String fieldName, char newvalue) throws HAWQException {
		setChar(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setChar(int fieldIndex, char newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = String.valueOf(newvalue); // convert char to string
	}

	/**
	 * Retrieves whether the value of the designated column in the current
	 * record is null.
	 *
	 * @param fieldName the name of the field
	 * @return whether the value is null
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public boolean isNull(String fieldName) throws HAWQException {
		return isNull(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves whether the value of the designated column in the current
	 * record is null.
	 *
	 * @param fieldIndex the index of the field
	 * @return whether the value is null
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public boolean isNull(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return values[fieldIndex - 1] == null;
	}

	/**
	 * Set the value of the designated column in the current record to null.
	 *
	 * @param fieldName the name of the field
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setNull(String fieldName) throws HAWQException {
		setNull(schema.getFieldIndex(fieldName));
	}

	/**
	 * Set the value of the designated column in the current record to null.
	 *
	 * @param fieldIndex the index of the field
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setNull(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = null;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Timestamp in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public Timestamp getTimestamp(String fieldName) throws HAWQException {
		return getTimestamp(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Timestamp in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public Timestamp getTimestamp(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (Timestamp) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setTimestamp(String fieldName, Timestamp newvalue)
			throws HAWQException {
		setTimestamp(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setTimestamp(int fieldIndex, Timestamp newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Time in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public Time getTime(String fieldName) throws HAWQException {
		return getTime(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Time in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public Time getTime(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (Time) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setTime(String fieldName, Time newvalue) throws HAWQException {
		setTime(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setTime(int fieldIndex, Time newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Date in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public Date getDate(String fieldName) throws HAWQException {
		return getDate(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Date in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public Date getDate(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (Date) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setDate(String fieldName, Date newvalue) throws HAWQException {
		setDate(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setDate(int fieldIndex, Date newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * BigDecimal in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public BigDecimal getBigDecimal(String fieldName) throws HAWQException {
		return getBigDecimal(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * BigDecimal in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public BigDecimal getBigDecimal(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (BigDecimal) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setBigDecimal(String fieldName, BigDecimal newvalue)
			throws HAWQException {
		setBigDecimal(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setBigDecimal(int fieldIndex, BigDecimal newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * Array in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public Array getArray(String fieldName) throws HAWQException {
		return getArray(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * Array in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public Array getArray(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQArray) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setArray(String fieldName, Array newvalue) throws HAWQException {
		setArray(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setArray(int fieldIndex, Array newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		if (!(newvalue instanceof HAWQArray)) {
			throw new HAWQException("only support HAWQArray instance");
		}
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQRecord in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQRecord getField(String fieldName) throws HAWQException {
		return getField(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQRecord in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQRecord getField(int fieldIndex) throws HAWQException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setField(String fieldName, HAWQRecord newvalue)
			throws HAWQException {
		setField(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setField(int fieldIndex, HAWQRecord newvalue)
			throws HAWQException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQBox in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQBox getBox(String fieldName) throws HAWQException {
		return getBox(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQBox in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQBox getBox(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQBox) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setBox(String fieldName, HAWQBox newvalue) throws HAWQException {
		setBox(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setBox(int fieldIndex, HAWQBox newvalue) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQCircle in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQCircle getCircle(String fieldName) throws HAWQException {
		return getCircle(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQCircle in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQCircle getCircle(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQCircle) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setCircle(String fieldName, HAWQCircle newvalue)
			throws HAWQException {
		setCircle(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setCircle(int fieldIndex, HAWQCircle newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQInterval in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQInterval getInterval(String fieldName) throws HAWQException {
		return getInterval(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQInterval in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQInterval getInterval(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQInterval) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setInterval(String fieldName, HAWQInterval newvalue)
			throws HAWQException {
		setInterval(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setInterval(int fieldIndex, HAWQInterval newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQLseg in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQLseg getLseg(String fieldName) throws HAWQException {
		return getLseg(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQLseg in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQLseg getLseg(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQLseg) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setLseg(String fieldName, HAWQLseg newvalue)
			throws HAWQException {
		setLseg(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setLseg(int fieldIndex, HAWQLseg newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPath in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQPath getPath(String fieldName) throws HAWQException {
		return getPath(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPath in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQPath getPath(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQPath) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setPath(String fieldName, HAWQPath newvalue)
			throws HAWQException {
		setPath(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setPath(int fieldIndex, HAWQPath newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPoint in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQPoint getPoint(String fieldName) throws HAWQException {
		return getPoint(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPoint in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQPoint getPoint(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQPoint) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setPoint(String fieldName, HAWQPoint newvalue)
			throws HAWQException {
		setPoint(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setPoint(int fieldIndex, HAWQPoint newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPolygon in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQPolygon getPolygon(String fieldName) throws HAWQException {
		return getPolygon(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPolygon in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQPolygon getPolygon(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQPolygon) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setPolygon(String fieldName, HAWQPolygon newvalue)
			throws HAWQException {
		setPolygon(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setPolygon(int fieldIndex, HAWQPolygon newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQMacaddr in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQMacaddr getMacaddr(String fieldName) throws HAWQException {
		return getMacaddr(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQMacaddr in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQMacaddr getMacaddr(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQMacaddr) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setMacaddr(String fieldName, HAWQMacaddr newvalue)
			throws HAWQException {
		setMacaddr(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setMacaddr(int fieldIndex, HAWQMacaddr newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQInet in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQInet getInet(String fieldName) throws HAWQException {
		return getInet(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQInet in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQInet getInet(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQInet) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setInet(String fieldName, HAWQInet newvalue)
			throws HAWQException {
		setInet(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setInet(int fieldIndex, HAWQInet newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQCidr in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQCidr getCidr(String fieldName) throws HAWQException {
		return getCidr(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQCidr in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQCidr getCidr(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQCidr) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setCidr(String fieldName, HAWQCidr newvalue)
			throws HAWQException {
		setCidr(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setCidr(int fieldIndex, HAWQCidr newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQVarbit in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQVarbit getVarbit(String fieldName) throws HAWQException {
		return getVarbit(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQVarbit in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQVarbit getVarbit(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		Object val = values[fieldIndex - 1];
		return val == null ? null : (HAWQVarbit) val;
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setVarbit(String fieldName, HAWQVarbit newvalue)
			throws HAWQException {
		setVarbit(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setVarbit(int fieldIndex, HAWQVarbit newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQVarbit in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public HAWQVarbit getBit(String fieldName) throws HAWQException {
		return getBit(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQVarbit in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public HAWQVarbit getBit(int fieldIndex) throws HAWQException {
		return getVarbit(fieldIndex);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldName the name of the field
	 * @param newvalue  new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setBit(String fieldName, HAWQVarbit newvalue)
			throws HAWQException {
		setBit(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 *
	 * @param fieldIndex the index of the field
	 * @param newvalue   new value to set
	 * @throws HAWQException if fieldIndex is not valid
	 */
	public void setBit(int fieldIndex, HAWQVarbit newvalue)
			throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex - 1] = newvalue;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * Object in the Java programming language.
	 *
	 * @param fieldName the name of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldName is not valid; if fail to get value from file
	 */
	public Object getObject(String fieldName) throws HAWQException {
		return getObject(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * Object in the Java programming language.
	 *
	 * @param fieldIndex the index of the field
	 * @return the field value; if the value is SQL <code>NULL</code>, the
	 * value returned is <code>null</code>
	 * @throws HAWQException if fieldIndex is not valid; if fail to get value from file
	 */
	public Object getObject(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return values[fieldIndex - 1];
	}

	/**
	 * Reset the record to initial state
	 */
	public void reset() {
		for (int i = 0; i < values.length; i++) {
			values[i] = null;
		}
	}

	//----------------- Formatting Methods -------------------
	protected static boolean toBoolean(String s) {
		if (s != null) {
			s = s.trim();

			if (s.equalsIgnoreCase("t") || s.equalsIgnoreCase("true") || s.equals("1"))
				return true;

			if (s.equalsIgnoreCase("f") || s.equalsIgnoreCase("false") || s.equals("0"))
				return false;

			try {
				if (Double.valueOf(s).doubleValue() == 1)
					return true;
			} catch (NumberFormatException e) {
			}
		}
		return false;  // SQL NULL
	}

	private static final BigInteger BYTEMAX = new BigInteger(Byte.toString(Byte.MAX_VALUE));
	private static final BigInteger BYTEMIN = new BigInteger(Byte.toString(Byte.MIN_VALUE));

	protected static byte toByte(String s) throws HAWQException {
		if (s != null) {
			s = s.trim();
			if (s.length() == 0)
				return 0;
			try {
				// try the optimal parse
				return Byte.parseByte(s);
			} catch (NumberFormatException e) {
				// didn't work, assume the column is not a byte
				try {
					BigDecimal n = new BigDecimal(s);
					BigInteger i = n.toBigInteger();

					int gt = i.compareTo(BYTEMAX);
					int lt = i.compareTo(BYTEMIN);

					if (gt > 0 || lt < 0) {
						throw new HAWQException("Bad value for type byte : " + s);
					}
					return i.byteValue();
				} catch (NumberFormatException ex) {
					throw new HAWQException("Bad value for type byte : " + s);
				}
			}
		}
		return 0; // SQL NULL
	}

	private static final BigInteger SHORTMAX = new BigInteger(Short.toString(Short.MAX_VALUE));
	private static final BigInteger SHORTMIN = new BigInteger(Short.toString(Short.MIN_VALUE));

	protected static short toShort(String s) throws HAWQException {
		if (s != null) {
			s = s.trim();
			try {
				return Short.parseShort(s);
			} catch (NumberFormatException e) {
				try {
					BigDecimal n = new BigDecimal(s);
					BigInteger i = n.toBigInteger();
					int gt = i.compareTo(SHORTMAX);
					int lt = i.compareTo(SHORTMIN);

					if (gt > 0 || lt < 0) {
						throw new HAWQException("Bad value for type short : " + s);
					}
					return i.shortValue();

				} catch (NumberFormatException ne) {
					throw new HAWQException("Bad value for type short : " + s);
				}
			}
		}
		return 0; // SQL NULL
	}

	private static final BigInteger INTMAX = new BigInteger(Integer.toString(Integer.MAX_VALUE));
	private static final BigInteger INTMIN = new BigInteger(Integer.toString(Integer.MIN_VALUE));

	protected static int toInt(String s) throws HAWQException {
		if (s != null) {
			try {
				s = s.trim();
				return Integer.parseInt(s);
			} catch (NumberFormatException e) {
				try {
					BigDecimal n = new BigDecimal(s);
					BigInteger i = n.toBigInteger();

					int gt = i.compareTo(INTMAX);
					int lt = i.compareTo(INTMIN);

					if (gt > 0 || lt < 0) {
						throw new HAWQException("Bad value for type int : " + s);
					}
					return i.intValue();

				} catch (NumberFormatException ne) {
					throw new HAWQException("Bad value for type int : " + s);
				}
			}
		}
		return 0;  // SQL NULL
	}

	private final static BigInteger LONGMAX = new BigInteger(Long.toString(Long.MAX_VALUE));
	private final static BigInteger LONGMIN = new BigInteger(Long.toString(Long.MIN_VALUE));

	protected static long toLong(String s) throws HAWQException {
		if (s != null) {
			try {
				s = s.trim();
				return Long.parseLong(s);
			} catch (NumberFormatException e) {
				try {
					BigDecimal n = new BigDecimal(s);
					BigInteger i = n.toBigInteger();
					int gt = i.compareTo(LONGMAX);
					int lt = i.compareTo(LONGMIN);

					if (gt > 0 || lt < 0) {
						throw new HAWQException("Bad value for type long : " + s);
					}
					return i.longValue();
				} catch (NumberFormatException ne) {
					throw new HAWQException("Bad value for type long : " + s);
				}
			}
		}
		return 0;  // SQL NULL
	}

	protected static float toFloat(String s) throws HAWQException {
		if (s != null) {
			try {
				s = s.trim();
				return Float.parseFloat(s);
			} catch (NumberFormatException e) {
				throw new HAWQException("Bad value for type float : " + s);
			}
		}
		return 0;  // SQL NULL
	}

	protected static double toDouble(String s) throws HAWQException {
		if (s != null) {
			try {
				s = s.trim();
				return Double.parseDouble(s);
			} catch (NumberFormatException e) {
				throw new HAWQException("Bad value for type double : " + s);
			}
		}
		return 0;  // SQL NULL
	}
}
