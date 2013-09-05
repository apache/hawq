package com.pivotal.hawq.mapreduce.parquet;


import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
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
 * User: gaod1
 * Date: 8/8/13
 */
public class HAWQParquetRecord extends HAWQRecord {

	private Object[] values;
	private int pos;

	public HAWQParquetRecord(HAWQSchema hawqSchema) {
		super(hawqSchema);
		this.values = new Object[hawqSchema.getFieldCount()];
	}

	private void checkFieldIndex(int fieldIndex) throws HAWQException {
		if (fieldIndex < 1 || fieldIndex > values.length)
			throw new HAWQException(String.format("index out of range (%d, %d)", 1, values.length));
	}

	@Override
	public boolean getBoolean(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (Boolean) values[fieldIndex-1];
	}

	@Override
	public void setBoolean(int fieldIndex, boolean x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public byte getByte(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (Byte) values[fieldIndex-1];
	}

	@Override
	public void setByte(int fieldIndex, byte x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public byte[] getBytes(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (byte[]) values[fieldIndex-1];
	}

	@Override
	public void setBytes(int fieldIndex, byte[] x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public double getDouble(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (Double) this.values[fieldIndex-1];
	}

	@Override
	public void setDouble(int fieldIndex, double x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public float getFloat(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (Float) this.values[fieldIndex-1];
	}

	@Override
	public void setFloat(int fieldIndex, float x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public int getInt(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (Integer) this.values[fieldIndex-1];
	}

	@Override
	public void setInt(int fieldIndex, int x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public long getLong(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (Long) this.values[fieldIndex-1];
	}

	@Override
	public void setLong(int fieldIndex, long x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public short getShort(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (Short) this.values[fieldIndex-1];
	}

	@Override
	public void setShort(int fieldIndex, short x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public String getString(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (String) this.values[fieldIndex-1];
	}

	@Override
	public void setString(int fieldIndex, String x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public boolean isNull(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return this.values[fieldIndex-1] == null;
	}

	@Override
	public void setNull(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = null;
	}

	@Override
	public Timestamp getTimestamp(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (Timestamp) this.values[fieldIndex-1];
	}

	@Override
	public void setTimestamp(int fieldIndex, Timestamp x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public Time getTime(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (Time) this.values[fieldIndex-1];
	}

	@Override
	public void setTime(int fieldIndex, Time x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public Date getDate(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (Date) this.values[fieldIndex-1];
	}

	@Override
	public void setDate(int fieldIndex, Date x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public BigDecimal getBigDecimal(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (BigDecimal) this.values[fieldIndex-1];
	}

	@Override
	public void setBigDecimal(int fieldIndex, BigDecimal x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public Array getArray(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		throw new UnsupportedOperationException();
	}

	@Override
	public void setArray(int fieldIndex, Array x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		throw new UnsupportedOperationException();
	}

	@Override
	public HAWQRecord getField(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return (HAWQRecord) this.values[fieldIndex-1];
	}

	@Override
	public void setField(int fieldIndex, HAWQRecord x) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		this.values[fieldIndex-1] = x;
	}

	@Override
	public Object getObject(int fieldIndex) throws HAWQException {
		this.checkFieldIndex(fieldIndex);
		return this.values[fieldIndex-1];
	}

	@Override
	public void reset() {
		this.pos = 0;
	}

	@Override
	public HAWQBox getBox(int fieldIndex) throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBox(int fieldIndex, HAWQBox box) throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public HAWQCircle getCircle(int fieldIndex) throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCircle(int fieldIndex, HAWQCircle circle)
			throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public HAWQInterval getInterval(int fieldIndex) throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void setInterval(int fieldIndex, HAWQInterval interval)
			throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public HAWQLseg getLseg(int fieldIndex) throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void setLseg(int fieldIndex, HAWQLseg lseg) throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public HAWQPath getPath(int fieldIndex) throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void setPath(int fieldIndex, HAWQPath path) throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public HAWQPoint getPoint(int fieldIndex) throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void setPoint(int fieldIndex, HAWQPoint point) throws HAWQException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public char getChar(int fieldIndex) throws HAWQException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setChar(int fieldIndex, char newvalue) throws HAWQException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public HAWQPolygon getPolygon(int fieldIndex) throws HAWQException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setPolygon(int fieldIndex, HAWQPolygon newvalue)
			throws HAWQException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public HAWQMacaddr getMacaddr(int fieldIndex) throws HAWQException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setMacaddr(int fieldIndex, HAWQMacaddr newvalue)
			throws HAWQException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public HAWQInet getInet(int fieldIndex) throws HAWQException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setInet(int fieldIndex, HAWQInet newvalue) throws HAWQException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public HAWQCidr getCidr(int fieldIndex) throws HAWQException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setCidr(int fieldIndex, HAWQCidr newvalue) throws HAWQException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public HAWQVarbit getVarbit(int fieldIndex) throws HAWQException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setVarbit(int fieldIndex, HAWQVarbit newvalue)
			throws HAWQException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public HAWQVarbit getBit(int fieldIndex) throws HAWQException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setBit(int fieldIndex, HAWQVarbit newvalue)
			throws HAWQException
	{
		// TODO Auto-generated method stub
		
	}
}
