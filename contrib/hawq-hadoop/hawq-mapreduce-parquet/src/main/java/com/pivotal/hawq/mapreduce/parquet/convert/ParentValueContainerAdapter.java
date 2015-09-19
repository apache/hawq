package com.pivotal.hawq.mapreduce.parquet.convert;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.datatype.*;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public abstract class ParentValueContainerAdapter implements ParentValueContainer {
	@Override
	public void setBoolean(boolean x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBit(HAWQVarbit x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setByte(byte x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBytes(byte[] x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setShort(short x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setInt(int x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setLong(long x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setFloat(float x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDouble(double x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBigDecimal(BigDecimal x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setString(String x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDate(Date x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTime(Time x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTimestamp(Timestamp x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setInterval(HAWQInterval x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setPoint(HAWQPoint x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setLseg(HAWQLseg x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBox(HAWQBox x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCircle(HAWQCircle x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setPath(HAWQPath x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setPolygon(HAWQPolygon x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMacaddr(HAWQMacaddr x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setInet(HAWQInet x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCidr(HAWQCidr x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setArray(Array x) throws HAWQException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setField(HAWQRecord x) throws HAWQException {
		throw new UnsupportedOperationException();
	}
}
