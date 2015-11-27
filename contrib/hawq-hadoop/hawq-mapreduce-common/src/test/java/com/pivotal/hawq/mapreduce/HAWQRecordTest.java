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
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import junit.framework.Assert;
import org.junit.Test;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField.PrimitiveType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

public class HAWQRecordTest {

	private static final double DELTA = 1e-6;

	private HAWQSchema newSchema(PrimitiveType type) {
		return new HAWQSchema("schema", HAWQSchema.required_field(type, "c1"));
	}

	@Test
	public void testReset() throws Exception {
		HAWQSchema schema = new HAWQSchema("schema",
										   HAWQSchema.required_field(PrimitiveType.INT4, "c1"),
										   HAWQSchema.required_field(PrimitiveType.TEXT, "c2"));
		HAWQRecord record = new HAWQRecord(schema);

		record.setInt("c1", 1);
		record.setString("c2", "hello");

		for (int i = 1; i <= schema.getFieldCount(); i++) {
			Assert.assertNotNull(record.getObject(i));
		}

		record.reset();

		for (int i = 1; i <= schema.getFieldCount(); i++) {
			Assert.assertNull(record.getObject(i));
		}
	}

	@Test
	public void testIndexOutOfRange() throws Exception {
		HAWQSchema schema = new HAWQSchema("schema",
										   HAWQSchema.required_field(PrimitiveType.INT4, "c1"),
										   HAWQSchema.required_field(PrimitiveType.TEXT, "c2"));
		HAWQRecord record = new HAWQRecord(schema);

		record.isNull(1);
		record.isNull(2);

		try {
			record.isNull(0);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("index out of range [1, 2]", e.getMessage());
		}

		try {
			record.isNull(3);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("index out of range [1, 2]", e.getMessage());
		}
	}

	@Test
	public void testGetObject() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		Assert.assertNull(record.getObject("c1"));

		record.setString("c1", "hello");
		Assert.assertEquals("hello", record.getObject("c1").toString());
	}

	@Test
	public void testGetBit() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.BIT));
		Assert.assertNull(record.getBit("c1"));

		HAWQVarbit bit = new HAWQVarbit("1");
		record.setBit("c1", bit);
		Assert.assertEquals(bit, record.getBit("c1"));
	}

	@Test
	public void testGetVarbit() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.VARBIT));
		Assert.assertNull(record.getVarbit("c1"));

		HAWQVarbit varbit = new HAWQVarbit("1010");
		record.setVarbit("c1", varbit);
		Assert.assertEquals(varbit, record.getVarbit("c1"));
	}

	@Test
	public void testGetCidr() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.CIDR));
		Assert.assertNull(record.getCidr("c1"));

		HAWQCidr cidr = new HAWQCidr(HAWQInet.InetType.IPV4,
									 new byte[] {(byte) 192, (byte) 168, 0, 0},
									 (short) 24);
		record.setCidr("c1", cidr);
		Assert.assertEquals(cidr, record.getCidr("c1"));
	}

	@Test
	public void testGetInet() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.INET));
		Assert.assertNull(record.getInet("c1"));

		HAWQInet inet = new HAWQInet("192.168.100.128/25");
		record.setInet("c1", inet);
		Assert.assertEquals(inet, record.getInet("c1"));
	}

	@Test
	public void testGetMacaddr() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.MACADDR));
		Assert.assertNull(record.getMacaddr("c1"));

		HAWQMacaddr macaddr = new HAWQMacaddr(new byte[] {0x00, 0x11, 0x22, 0x33, 0x44, 0x55});
		record.setMacaddr("c1", macaddr);
		Assert.assertEquals(macaddr, record.getMacaddr("c1"));
	}

	@Test
	public void testGetPolygon() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.POLYGON));
		Assert.assertNull(record.getPolygon("c1"));

		HAWQPolygon polygon = new HAWQPolygon("((1,1),(3,2),(4,5))");
		record.setPolygon("c1", polygon);
		Assert.assertEquals(polygon, record.getPolygon("c1"));
	}

	@Test
	public void testGetPoint() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.POINT));
		Assert.assertNull(record.getPoint("c1"));

		HAWQPoint point = new HAWQPoint("(1,2)");
		record.setPoint("c1", point);
		Assert.assertEquals(point, record.getPoint("c1"));
	}

	@Test
	public void testGetPath() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.PATH));
		Assert.assertNull(record.getPath("c1"));

		HAWQPath path = new HAWQPath("((1,1),(2,3),(4,5))");
		record.setPath("c1", path);
		Assert.assertEquals(path, record.getPath("c1"));
	}

	@Test
	public void testGetLseg() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.LSEG));
		Assert.assertNull(record.getLseg("c1"));

		HAWQLseg lseg = new HAWQLseg("[(0,0),(6,6)]");
		record.setLseg("c1", lseg);
		Assert.assertEquals(lseg, record.getLseg("c1"));
	}

	@Test
	public void testGetInterval() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.INTERVAL));
		Assert.assertNull(record.getInterval("c1"));

		HAWQInterval interval = new HAWQInterval(1, 0, 0, 0, 0, 0);
		record.setInterval("c1", interval);
		Assert.assertEquals(interval, record.getInterval("c1"));
	}

	@Test
	public void testGetCircle() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.CIRCLE));
		Assert.assertNull(record.getCircle("c1"));

		HAWQCircle circle = new HAWQCircle("<(1,2),3>");
		record.setCircle("c1", circle);
		Assert.assertEquals(circle, record.getCircle("c1"));
	}

	@Test
	public void testGetBox() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.BOX));
		Assert.assertNull(record.getBox("c1"));

		HAWQBox box = new HAWQBox("(0,1),(2,3)");
		record.setBox("c1", box);
		Assert.assertEquals(box, record.getBox("c1"));
	}

	@Test
	public void testGetField() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.INT4));
		try {
			record.getField("c1");
			Assert.fail();
		} catch (UnsupportedOperationException e) {}

		try {
			record.setField("c1", null);
			Assert.fail();
		} catch (UnsupportedOperationException e) {}
	}

	@Test
	public void testGetArray() throws Exception {
		HAWQSchema schema = new HAWQSchema(
				"schema", HAWQSchema.required_field_array(PrimitiveType.INT4, "ids"));
		HAWQRecord record = new HAWQRecord(schema);
		Assert.assertNull(record.getArray("ids"));
	}

	@Test
	public void testGetBigDecimal() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.NUMERIC));
		Assert.assertNull(record.getBigDecimal("c1"));

		BigDecimal val = new BigDecimal("3.1415");
		record.setBigDecimal("c1", val);
		Assert.assertEquals(val, record.getBigDecimal("c1"));
	}

	@Test
	public void testGetDate() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.DATE));
		Assert.assertNull(record.getDate("c1"));

		Date date = new Date(System.currentTimeMillis());
		record.setDate("c1", date);
		Assert.assertEquals(date, record.getDate("c1"));
	}

	@Test
	public void testGetTime() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.TIME));
		Assert.assertNull(record.getTime("c1"));

		Time time = new Time(System.currentTimeMillis());
		record.setTime("c1", time);
		Assert.assertEquals(time, record.getTime("c1"));
	}

	@Test
	public void testGetTimestamp() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.TIMESTAMP));
		Assert.assertNull(record.getTimestamp("c1"));

		Timestamp ts = new Timestamp(System.currentTimeMillis());
		record.setTimestamp("c1", ts);
		Assert.assertEquals(ts, record.getTimestamp("c1"));
	}

	@Test
	public void testIsNull() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.INT4));
		Assert.assertTrue(record.isNull(1));
		Assert.assertTrue(record.isNull("c1"));

		record.setInt(1, 0);
		Assert.assertFalse(record.isNull(1));
	}

	@Test
	public void testGetChar() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.CHAR));
		Assert.assertEquals(0, record.getChar("c1"));

		record.setChar("c1", 'a');
		Assert.assertEquals('a', record.getChar("c1"));

		record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		record.setString(1, "a");
		Assert.assertEquals('a', record.getChar(1));

		record.setString(1, "hello");
		try {
			record.getChar(1);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("Bad value for type char : hello", e.getMessage());
		}

		record = new HAWQRecord(newSchema(PrimitiveType.DATE));
		record.setDate(1, new Date(System.currentTimeMillis()));
		try {
			record.getChar(1);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("cannot use of getChar on field type: DATE", e.getMessage());
		}
	}

	@Test
	public void testGetString() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		Assert.assertNull(record.getString("c1"));

		record.setString("c1", "abc");
		Assert.assertEquals("abc", record.getString("c1"));

		record = new HAWQRecord(newSchema(PrimitiveType.INT4));
		Assert.assertNull(record.getString(1));

		record.setInt(1, 100);
		Assert.assertEquals("100", record.getString(1));
	}

	@Test
	public void testGetShort() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.INT2));
		Assert.assertEquals(0, record.getShort("c1"));

		record.setShort("c1", (short) 100);
		Assert.assertEquals((short) 100, record.getShort("c1"));

		record = new HAWQRecord(newSchema(PrimitiveType.INT4));
		record.setInt(1, Short.MAX_VALUE);
		Assert.assertEquals(Short.MAX_VALUE, record.getShort(1));

		record.setInt(1, Integer.MAX_VALUE);
		try {
			record.getShort(1);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("Bad value for type short : " + Integer.MAX_VALUE, e.getMessage());
		}
	}

	@Test
	public void testGetLong() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.INT8));
		Assert.assertEquals(0, record.getLong("c1"));

		record.setLong("c1", Long.MAX_VALUE);
		Assert.assertEquals(Long.MAX_VALUE, record.getLong("c1"));

		record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		record.setString(1, "123.456");
		Assert.assertEquals(123, record.getLong(1));

		record.setString(1, "hello");
		try {
			record.getLong(1);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("Bad value for type long : hello", e.getMessage());
		}
	}

	@Test
	public void testGetInt() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.INT4));
		Assert.assertEquals(0, record.getInt("c1"));

		record.setInt("c1", 123456);
		Assert.assertEquals(123456, record.getInt("c1"));

		record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		record.setString(1, "-667788");
		Assert.assertEquals(-667788, record.getInt(1));

		record.setString(1, "hello");
		try {
			record.getInt(1);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("Bad value for type int : hello", e.getMessage());
		}

		record = new HAWQRecord(newSchema(PrimitiveType.FLOAT8));
		record.setDouble(1, 3.14);
		Assert.assertEquals(3, record.getInt(1));

		record.setDouble(1, 99999999999999999.0);
		try {
			record.getInt(1);
			Assert.fail();
		} catch (HAWQException e) {}
	}

	@Test
	public void testGetFloat() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.FLOAT4));
		Assert.assertEquals(0.0f, record.getFloat("c1"), DELTA);

		// test getters and setters
		record.setFloat("c1", -1.23f);
		Assert.assertEquals(-1.23f, record.getFloat("c1"), DELTA);

		// test getter on field of other types
		record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		record.setString(1, "3.1415");
		Assert.assertEquals(3.1415f, record.getFloat(1), DELTA);

		record = new HAWQRecord(newSchema(PrimitiveType.INT4));
		record.setInt(1, 100);
		Assert.assertEquals(100.0f, record.getFloat(1), DELTA);

		record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		record.setString(1, "hello");
		try {
			record.getFloat(1);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("Bad value for type float : hello", e.getMessage());
		}
	}

	@Test
	public void testGetDouble() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.FLOAT8));
		Assert.assertEquals(0.0f, record.getDouble("c1"), DELTA);

		// test getters and setters
		record.setDouble("c1", 1.23);
		Assert.assertEquals(1.23, record.getDouble("c1"), DELTA);

		// test getter on field of other types
		record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		record.setString(1, "3.1415");
		Assert.assertEquals(3.1415, record.getDouble(1), DELTA);

		record = new HAWQRecord(newSchema(PrimitiveType.INT4));
		record.setInt(1, 100);
		Assert.assertEquals(100.0, record.getDouble(1), DELTA);

		record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		record.setString(1, "hello");
		try {
			record.getDouble(1);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("Bad value for type double : hello", e.getMessage());
		}
	}

	@Test
	public void testGetBytes() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.BYTEA));
		Assert.assertNull(record.getBytes("c1"));

		byte[] bytes = new byte[] {0x11, 0x22, 0x33};
		record.setBytes("c1", bytes);
		Assert.assertTrue(Arrays.equals(bytes, record.getBytes("c1")));
	}

	@Test
	public void testGetByte() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.INT4));
		Assert.assertEquals(0, record.getByte("c1"));

		// test getters and setters
		record.setByte("c1", (byte) 0x88);
		Assert.assertEquals((byte) 0x88, record.getByte("c1"));

		// test getter on field of other types
		record.setInt(1, 300);
		try {
			record.getByte(1);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("Bad value for type byte : 300", e.getMessage());
		}

		record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		record.setString(1, "16");
		Assert.assertEquals((byte) 16, record.getByte(1));

		record.setString(1, "hello");
		try {
			record.getByte(1);
			Assert.fail();
		} catch (HAWQException e) {
			Assert.assertEquals("Bad value for type byte : hello", e.getMessage());
		}
	}

	@Test
	public void testGetBoolean() throws Exception {
		HAWQRecord record = new HAWQRecord(newSchema(PrimitiveType.BOOL));
		Assert.assertEquals(false, record.getBoolean("c1"));

		// test getters and setters
		record.setBoolean("c1", true);
		Assert.assertEquals(true, record.getBoolean("c1"));

		// test getter on field of other types
		record = new HAWQRecord(newSchema(PrimitiveType.INT4));
		record.setInt("c1", 1);
		Assert.assertEquals(true, record.getBoolean(1));

		record.setInt("c1", 100);
		Assert.assertEquals(false, record.getBoolean(1));

		record = new HAWQRecord(newSchema(PrimitiveType.TEXT));
		record.setString(1, "t");
		Assert.assertEquals(true, record.getBoolean(1));

		record.setString(1, "false");
		Assert.assertEquals(false, record.getBoolean(1));
	}
}
