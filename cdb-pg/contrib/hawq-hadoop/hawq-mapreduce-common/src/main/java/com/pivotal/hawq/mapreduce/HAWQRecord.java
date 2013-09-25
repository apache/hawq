package com.pivotal.hawq.mapreduce;

import com.pivotal.hawq.mapreduce.datatype.HAWQBox;
import com.pivotal.hawq.mapreduce.datatype.HAWQCidr;
import com.pivotal.hawq.mapreduce.datatype.HAWQCircle;
import com.pivotal.hawq.mapreduce.datatype.HAWQInet;
import com.pivotal.hawq.mapreduce.datatype.HAWQInterval;
import com.pivotal.hawq.mapreduce.datatype.HAWQLseg;
import com.pivotal.hawq.mapreduce.datatype.HAWQMacaddr;
import com.pivotal.hawq.mapreduce.datatype.HAWQPath;
import com.pivotal.hawq.mapreduce.datatype.HAWQPoint;
import com.pivotal.hawq.mapreduce.datatype.HAWQPolygon;
import com.pivotal.hawq.mapreduce.datatype.HAWQVarbit;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Store schema and values for a record in database.
 * 
 * User can use getXXX method in input format.
 */
public abstract class HAWQRecord
{

	private HAWQSchema schema;

	protected HAWQRecord(HAWQSchema schema)
	{
		this.schema = schema;
	}

	/**
	 * Get schema of this record
	 * 
	 * @return the schema in this record
	 */
	public HAWQSchema getSchema()
	{
		return schema;
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * boolean in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public boolean getBoolean(String fieldName) throws HAWQException
	{
		return getBoolean(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * boolean in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract boolean getBoolean(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setBoolean(String fieldName, boolean newvalue)
			throws HAWQException
	{
		setBoolean(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setBoolean(int fieldIndex, boolean newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * byte in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public byte getByte(String fieldName) throws HAWQException
	{
		return getByte(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * byte in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract byte getByte(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setByte(String fieldName, byte newvalue) throws HAWQException
	{
		setByte(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setByte(int fieldIndex, byte newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * byte[] in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public byte[] getBytes(String fieldName) throws HAWQException
	{
		return getBytes(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * byte[] in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract byte[] getBytes(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setBytes(String fieldName, byte[] newvalue)
			throws HAWQException
	{
		setBytes(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setBytes(int fieldIndex, byte[] newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * double in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public double getDouble(String fieldName) throws HAWQException
	{
		return getDouble(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * double in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract double getDouble(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setDouble(String fieldName, double newvalue)
			throws HAWQException
	{
		setDouble(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setDouble(int fieldIndex, double newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * float in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public float getFloat(String fieldName) throws HAWQException
	{
		return getFloat(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * float in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract float getFloat(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setFloat(String fieldName, float newvalue) throws HAWQException
	{
		setFloat(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setFloat(int fieldIndex, float newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * int in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public int getInt(String fieldName) throws HAWQException
	{
		return getInt(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * int in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract int getInt(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setInt(String fieldName, int newvalue) throws HAWQException
	{
		setInt(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setInt(int fieldIndex, int newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * long in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public long getLong(String fieldName) throws HAWQException
	{
		return getLong(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * long in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract long getLong(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setLong(String fieldName, long newvalue) throws HAWQException
	{
		setLong(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setLong(int fieldIndex, long newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * short in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public short getShort(String fieldName) throws HAWQException
	{
		return getShort(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * short in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract short getShort(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setShort(String fieldName, short newvalue) throws HAWQException
	{
		setShort(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setShort(int fieldIndex, short newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * String in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public String getString(String fieldName) throws HAWQException
	{
		return getString(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * String in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract String getString(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setString(String fieldName, String newvalue)
			throws HAWQException
	{
		setString(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setString(int fieldIndex, String newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * char in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public char getChar(String fieldName) throws HAWQException
	{
		return getChar(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * char in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract char getChar(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setChar(String fieldName, char newvalue) throws HAWQException
	{
		setChar(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setChar(int fieldIndex, char newvalue)
			throws HAWQException;

	/**
	 * Retrieves whether the value of the designated column in the current
	 * record is null.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return whether the value is null
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public boolean isNull(String fieldName) throws HAWQException
	{
		return isNull(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves whether the value of the designated column in the current
	 * record is null.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return whether the value is null
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract boolean isNull(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to null.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setNull(String fieldName) throws HAWQException
	{
		setNull(schema.getFieldIndex(fieldName));
	}

	/**
	 * Set the value of the designated column in the current record to null.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setNull(int fieldIndex) throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Timestamp in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public Timestamp getTimestamp(String fieldName) throws HAWQException
	{
		return getTimestamp(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Timestamp in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract Timestamp getTimestamp(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setTimestamp(String fieldName, Timestamp newvalue)
			throws HAWQException
	{
		setTimestamp(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setTimestamp(int fieldIndex, Timestamp newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Time in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public Time getTime(String fieldName) throws HAWQException
	{
		return getTime(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Time in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract Time getTime(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setTime(String fieldName, Time newvalue) throws HAWQException
	{
		setTime(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setTime(int fieldIndex, Time newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Date in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public Date getDate(String fieldName) throws HAWQException
	{
		return getDate(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * java.sql.Date in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract Date getDate(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setDate(String fieldName, Date newvalue) throws HAWQException
	{
		setDate(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setDate(int fieldIndex, Date newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * BigDecimal in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public BigDecimal getBigDecimal(String fieldName) throws HAWQException
	{
		return getBigDecimal(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * BigDecimal in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract BigDecimal getBigDecimal(int fieldIndex)
			throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setBigDecimal(String fieldName, BigDecimal newvalue)
			throws HAWQException
	{
		setBigDecimal(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setBigDecimal(int fieldIndex, BigDecimal newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * Array in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public Array getArray(String fieldName) throws HAWQException
	{
		return getArray(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * Array in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract Array getArray(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setArray(String fieldName, Array newvalue) throws HAWQException
	{
		setArray(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setArray(int fieldIndex, Array newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQRecord in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQRecord getField(String fieldName) throws HAWQException
	{
		return getField(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQRecord in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQRecord getField(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setField(String fieldName, HAWQRecord newvalue)
			throws HAWQException
	{
		setField(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setField(int fieldIndex, HAWQRecord newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQBox in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQBox getBox(String fieldName) throws HAWQException
	{
		return getBox(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQBox in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQBox getBox(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setBox(String fieldName, HAWQBox newvalue) throws HAWQException
	{
		setBox(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setBox(int fieldIndex, HAWQBox newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQCircle in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQCircle getCircle(String fieldName) throws HAWQException
	{
		return getCircle(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQCircle in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQCircle getCircle(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setCircle(String fieldName, HAWQCircle newvalue)
			throws HAWQException
	{
		setCircle(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setCircle(int fieldIndex, HAWQCircle newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQInterval in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQInterval getInterval(String fieldName) throws HAWQException
	{
		return getInterval(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQInterval in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQInterval getInterval(int fieldIndex)
			throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setInterval(String fieldName, HAWQInterval newvalue)
			throws HAWQException
	{
		setInterval(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setInterval(int fieldIndex, HAWQInterval newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQLseg in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQLseg getLseg(String fieldName) throws HAWQException
	{
		return getLseg(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQLseg in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQLseg getLseg(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setLseg(String fieldName, HAWQLseg newvalue)
			throws HAWQException
	{
		setLseg(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setLseg(int fieldIndex, HAWQLseg newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPath in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQPath getPath(String fieldName) throws HAWQException
	{
		return getPath(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPath in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQPath getPath(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setPath(String fieldName, HAWQPath newvalue)
			throws HAWQException
	{
		setPath(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setPath(int fieldIndex, HAWQPath newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPoint in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQPoint getPoint(String fieldName) throws HAWQException
	{
		return getPoint(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPoint in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQPoint getPoint(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setPoint(String fieldName, HAWQPoint newvalue)
			throws HAWQException
	{
		setPoint(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setPoint(int fieldIndex, HAWQPoint newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPolygon in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQPolygon getPolygon(String fieldName) throws HAWQException
	{
		return getPolygon(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQPolygon in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQPolygon getPolygon(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setPolygon(String fieldName, HAWQPolygon newvalue)
			throws HAWQException
	{
		setPolygon(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setPolygon(int fieldIndex, HAWQPolygon newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQMacaddr in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQMacaddr getMacaddr(String fieldName) throws HAWQException
	{
		return getMacaddr(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQMacaddr in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQMacaddr getMacaddr(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setMacaddr(String fieldName, HAWQMacaddr newvalue)
			throws HAWQException
	{
		setMacaddr(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setMacaddr(int fieldIndex, HAWQMacaddr newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQInet in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQInet getInet(String fieldName) throws HAWQException
	{
		return getInet(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQInet in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQInet getInet(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setInet(String fieldName, HAWQInet newvalue)
			throws HAWQException
	{
		setInet(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setInet(int fieldIndex, HAWQInet newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQCidr in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQCidr getCidr(String fieldName) throws HAWQException
	{
		return getCidr(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQCidr in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQCidr getCidr(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setCidr(String fieldName, HAWQCidr newvalue)
			throws HAWQException
	{
		setCidr(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setCidr(int fieldIndex, HAWQCidr newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQVarbit in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQVarbit getVarbit(String fieldName) throws HAWQException
	{
		return getVarbit(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQVarbit in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQVarbit getVarbit(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setVarbit(String fieldName, HAWQVarbit newvalue)
			throws HAWQException
	{
		setVarbit(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setVarbit(int fieldIndex, HAWQVarbit newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQVarbit in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public HAWQVarbit getBit(String fieldName) throws HAWQException
	{
		return getBit(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * HAWQVarbit in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract HAWQVarbit getBit(int fieldIndex) throws HAWQException;

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public void setBit(String fieldName, HAWQVarbit newvalue)
			throws HAWQException
	{
		setBit(schema.getFieldIndex(fieldName), newvalue);
	}

	/**
	 * Set the value of the designated column in the current record to new
	 * value.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @param newvalue
	 *            new value to set
	 * @throws HAWQException
	 *             if fieldIndex is not valid
	 */
	public abstract void setBit(int fieldIndex, HAWQVarbit newvalue)
			throws HAWQException;

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * Object in the Java programming language.
	 * 
	 * @param fieldName
	 *            the name of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldName is not valid; if fail to get value from file
	 */
	public Object getObject(String fieldName) throws HAWQException
	{
		return getObject(schema.getFieldIndex(fieldName));
	}

	/**
	 * Retrieves the value of the designated column in the current record as a
	 * Object in the Java programming language.
	 * 
	 * @param fieldIndex
	 *            the index of the field
	 * @return the field value
	 * @throws HAWQException
	 *             if fieldIndex is not valid; if fail to get value from file
	 */
	public abstract Object getObject(int fieldIndex) throws HAWQException;

	/**
	 * Reset the record to initial state
	 */
	public abstract void reset();
}
