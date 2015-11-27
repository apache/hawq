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

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public interface ParentValueContainer {

	///////////////////////////////////////////////////////////////////////
	// BOOL, BIT, VARBIT, BYTEA, INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC
	///////////////////////////////////////////////////////////////////////

	void setBoolean(boolean x) throws HAWQException;

	void setBit(HAWQVarbit x) throws HAWQException;

	void setByte(byte x) throws HAWQException;

	void setBytes(byte[] x) throws HAWQException;

	void setShort(short x) throws HAWQException;

	void setInt(int x) throws HAWQException;

	void setLong(long x) throws HAWQException;

	void setFloat(float x) throws HAWQException;

	void setDouble(double x) throws HAWQException;

	void setBigDecimal(BigDecimal x) throws HAWQException;

	///////////////////////////////////////////////////////////////////////
	// CHAR, BPCHAR, VARCHAR, TEXT, DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ, INTERVAL
	///////////////////////////////////////////////////////////////////////

	void setString(String x) throws HAWQException;

	void setDate(Date x) throws HAWQException;

	void setTime(Time x) throws HAWQException;

	void setTimestamp(Timestamp x) throws HAWQException;

	void setInterval(HAWQInterval x) throws HAWQException;

	///////////////////////////////////////////////////////////////////////
	// POINT, LSEG, BOX, CIRCLE, PATH, POLYGON, MACADDR, INET, CIDR, XML
	///////////////////////////////////////////////////////////////////////

	void setPoint(HAWQPoint x) throws HAWQException;

	void setLseg(HAWQLseg x) throws HAWQException;

	void setBox(HAWQBox x) throws HAWQException;

	void setCircle(HAWQCircle x) throws HAWQException;

	void setPath(HAWQPath x) throws HAWQException;

	void setPolygon(HAWQPolygon x) throws HAWQException;

	void setMacaddr(HAWQMacaddr x) throws HAWQException;

	void setInet(HAWQInet x) throws HAWQException;

	void setCidr(HAWQCidr x) throws HAWQException;

	///////////////////////////////////////////////////////////////////////
	// other
	///////////////////////////////////////////////////////////////////////

	void setArray(Array x) throws HAWQException;

	void setField(HAWQRecord x) throws HAWQException;

}
