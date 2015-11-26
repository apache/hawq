package com.pivotal.hawq.mapreduce.util;

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
import com.pivotal.hawq.mapreduce.datatype.HAWQArray;
import com.pivotal.hawq.mapreduce.datatype.HAWQBox;
import com.pivotal.hawq.mapreduce.datatype.HAWQCidr;
import com.pivotal.hawq.mapreduce.datatype.HAWQInet;
import com.pivotal.hawq.mapreduce.datatype.HAWQInterval;
import com.pivotal.hawq.mapreduce.datatype.HAWQPath;
import com.pivotal.hawq.mapreduce.datatype.HAWQPoint;
import com.pivotal.hawq.mapreduce.datatype.HAWQPolygon;
import com.pivotal.hawq.mapreduce.datatype.HAWQVarbit;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * This class is an util class and supply static methods for type converting.
 */
public abstract class HAWQConvertUtil
{
	//--------------------------------------------------------------------------------
	//----- Section: Date/Time/Timestamp/Interval
	//--------------------------------------------------------------------------------
	private static Calendar defaultCal = new GregorianCalendar();
	private static TimeZone defaultTz = defaultCal.getTimeZone();

	// We can't use Long.MAX_VALUE or Long.MIN_VALUE for java.sql.date
	// because this would break the 'normalization contract' of the
	// java.sql.Date API.
	// The follow values are the nearest MAX/MIN values with hour,
	// minute, second, millisecond set to 0 - this is used for
	// -infinity / infinity representation in Java
	private static final long DATE_POSITIVE_INFINITY = 9223372036825200000l;
	private static final long DATE_NEGATIVE_INFINITY = -9223372036832400000l;
	private static final long DATE_POSITIVE_SMALLER_INFINITY = 185543533774800000l;
	private static final long DATE_NEGATIVE_SMALLER_INFINITY = -185543533774800000l;

	/**
	 * Converts the given postgresql seconds to java seconds.
	 * Reverse engineered by inserting varying dates to postgresql
	 * and tuning the formula until the java dates matched.
	 *
	 * @param secs Postgresql seconds.
	 * @return Java seconds.
	 */
	private static long toJavaSecs(long secs) {
		// postgres epoc to java epoc
		secs += 946684800L;

		// Julian/Gregorian calendar cutoff point
		if (secs < -12219292800L) { // October 4, 1582 -> October 15, 1582
			secs += 86400 * 10;
			if (secs < -14825808000L) { // 1500-02-28 -> 1500-03-01
				int extraLeaps = (int) ((secs + 14825808000L) / 3155760000L);
				extraLeaps--;
				extraLeaps -= extraLeaps / 4;
				secs += extraLeaps * 86400L;
			}
		}
		return secs;
	}

	/**
	 * Convert from int to date. HAWQ stores date as int4 in DB,
	 * which is the julian day number since 2000/01/01.
	 * @param days
	 * @return
	 */
	public static Date toDate(int days) {
		long secs = toJavaSecs(days * 86400L);
		long millis = secs * 1000L;
		int offset = defaultTz.getOffset(millis);
		if (millis <= DATE_NEGATIVE_SMALLER_INFINITY) {
			millis = DATE_NEGATIVE_INFINITY;
			offset = 0;
		} else if (millis >= DATE_POSITIVE_SMALLER_INFINITY) {
			millis = DATE_POSITIVE_INFINITY;
			offset = 0;
		}
		return new Date(millis - offset);
	}

	/**
	 * Convert from long to time. HAWQ stored time as int8, which is the
	 * microseconds since 2000-01-01 00:00:00 GMT.
	 * @param time value stored in database
	 * @return java.sql.Time instance representing the stored value.
	 */
	public static Time toTime(long time) {
		long millis = time / 1000;
		int timeOffset = defaultTz.getOffset(millis);

		return new Time(millis - timeOffset);
	}

	/**
	 * Convert from 12 bytes to timetz. HAWQ stored timetz as 12 bytes, which is
	 * int8 time and int4 zone offset.
	 * @param bytes value stored in database
	 * @param offset offset of the byte array indicating the start of value
	 * @return java.sql.Time instance representing the stored value.
	 * @throws HAWQException
	 */
	public static Time toTimeTz(byte[] bytes, int offset) throws HAWQException {
		long time = bytesToLong(bytes, offset);
		long millis = time / 1000;

		int timeOffset = bytesToInt(bytes, offset + 8);
		timeOffset *= -1000;

		return new Time(millis - timeOffset);
	}

	/**
	 * Convert from long to timestamp/timestamptz. HAWQ stored timestamp/timestamptz as int8,
	 * which is the microseconds since 2000-01-01 00:00:00 GMT.
	 * @param time value stored in database
	 * @param isTimestamptz true if binary is in GMT.
	 * @return java.sql.Timestamp instance representing the stored value.
	 */
	public static Timestamp toTimestamp(long time, boolean isTimestamptz) {
		long secs = time / 1000000;
		int nanos = (int) (time - secs * 1000000);

		if (nanos < 0) {
			secs--;
			nanos += 1000000;
		}
		nanos *= 1000;

		secs = toJavaSecs(secs);
		long millis = secs * 1000L;
		if (!isTimestamptz) {
			millis -= defaultTz.getOffset(millis);
		}

		Timestamp result =  new Timestamp(millis);
		result.setNanos(nanos);
		return result;
	}

	/**
	 * Convert 16 bytes to interval
	 *
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset in byte array
	 * @return interval converted from byte array
	 * @throws HAWQException
	 *             when there is no 16 bytes from offset to end of bytes
	 */
	public static HAWQInterval bytesToInterval(byte[] bytes, int offset)
			throws HAWQException {
		if (bytes.length - offset < 16)
			throw new HAWQException("Need at least 16 bytes: offset is "
											+ offset + " while length of bytes is " + bytes.length);

		long time1 = bytesToLong(bytes, offset);
		int day = bytesToInt(bytes, offset + 8);
		int allmonths = bytesToInt(bytes, offset + 12);

		int microsecond = (int) (time1 % 1000);
		time1 /= 1000; // all milliseconds
		int millisecond = (int) (time1 % 1000);
		time1 /= 1000; // all seconds
		int second = (int) (time1 % 60);
		time1 /= 60; // all minutes
		int minute = (int) (time1 % 60);
		time1 /= 60; // all hours
		int hour = (int) (time1 % 24);
		int month = allmonths % 12;
		int year = allmonths / 12;

		return new HAWQInterval(year, month, day, hour, minute, second,
								millisecond, microsecond);
	}
	//--------------------------------------------------------------------------------
	//----- Date/Time/Timestamp/Interval END
	//--------------------------------------------------------------------------------

	/**
	 * Convert byte to boolean
	 * 
	 * @param in
	 *            one byte, 0 means false, 1 means true
	 * @return true if the byte is 1 and false if the byte is 0
	 * @throws HAWQException
	 *             when this byte is not 0 or 1
	 */
	public static boolean byteToBoolean(byte in) throws HAWQException
	{
		byte temp = in;
		boolean data;
		if (temp == 0)
			data = false;
		else if (temp == 1)
			data = true;
		else
			throw new HAWQException("Not a boolean value",
					HAWQException.WRONGFILEFORMAT_EXCEPTION);
		return data;
	}

	/**
	 * Convert two bytes to short
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset in byte array
	 * @return short value converted from byte array
	 * @throws HAWQException
	 *             when there is no 2 bytes from offset to end of bytes
	 */
	public static short bytesToShort(byte[] bytes, int offset)
			throws HAWQException
	{
		try
		{
			return (short) ((((short) bytes[offset + 1] & 0x00FF) << 8) | (((short) bytes[offset]) & 0x00FF));
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException("Need at least 2 bytes: offset is "
					+ offset + " while length of bytes is " + bytes.length);
		}
	}

	/**
	 * Convert three bytes to integer
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset in byte array
	 * @return int value converted from byte array
	 * @throws HAWQException
	 *             when there is no 3 bytes from offset to end of bytes
	 */
	public static int threeBytesToInt(byte[] bytes, int offset)
			throws HAWQException
	{
		try
		{
			return (0 << 24) | ((((int) bytes[offset + 2]) & 0x000000FF) << 16)
					| ((((int) bytes[offset + 1]) & 0x000000FF) << 8)
					| (((int) bytes[offset]) & 0x000000FF);
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException("Need at least 3 bytes: offset is "
					+ offset + " while length of bytes is " + bytes.length);
		}
	}

	/**
	 * Convert four bytes to integer
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset in byte array
	 * @return int value converted from byte array
	 * @throws HAWQException
	 *             when there is no 4 bytes from offset to end of bytes
	 */
	public static int bytesToInt(byte[] bytes, int offset) throws HAWQException
	{
		try
		{
			return ((((int) bytes[offset + 3]) & 0x000000FF) << 24)
					| ((((int) bytes[offset + 2]) & 0x000000FF) << 16)
					| ((((int) bytes[offset + 1]) & 0x000000FF) << 8)
					| (((int) bytes[offset]) & 0x000000FF);
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException("Need at least 4 bytes: offset is "
					+ offset + " while length of bytes is " + bytes.length);
		}
	}

	/**
	 * Convert four bytes to float
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset in byte array
	 * @return float value converted from byte array
	 * @throws HAWQException
	 *             when there is no 4 bytes from offset to end of bytes
	 */
	public static float bytesToFloat(byte[] bytes, int offset)
			throws HAWQException
	{
		return Float.intBitsToFloat(bytesToInt(bytes, offset));
	}

	/**
	 * Convert eight bytes to long
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset in byte array
	 * @return long value converted from byte array
	 * @throws HAWQException
	 *             when there is no 8 bytes from offset to end of bytes
	 */
	public static long bytesToLong(byte[] bytes, int offset)
			throws HAWQException
	{
		try
		{
			return ((((long) bytes[offset + 7]) & 0x00000000000000FFl) << 56)
					| ((((long) bytes[offset + 6]) & 0x00000000000000FFl) << 48)
					| ((((long) bytes[offset + 5]) & 0x00000000000000FFl) << 40)
					| ((((long) bytes[offset + 4]) & 0x00000000000000FFl) << 32)
					| ((((long) bytes[offset + 3]) & 0x00000000000000FFl) << 24)
					| ((((long) bytes[offset + 2]) & 0x00000000000000FFl) << 16)
					| ((((long) bytes[offset + 1]) & 0x00000000000000FFl) << 8)
					| (((long) bytes[offset]) & 0x00000000000000FFl);
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException("Need at least 8 bytes: offset is "
					+ offset + " while length of bytes is " + bytes.length);
		}
	}

	/**
	 * Convert eight bytes to double
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset in byte array
	 * @return double value converted from bytes
	 * @throws HAWQException
	 *             when there is no 8 bytes from offset to end of bytes
	 */
	public static double bytesToDouble(byte[] bytes, int offset)
			throws HAWQException
	{
		return Double.longBitsToDouble(bytesToLong(bytes, offset));
	}

	/**
	 * Convert bytes to string of big decimal
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset_numeric
	 *            offset in byte array
	 * @return String "NaN" (means 'not a number') or correct decimal converted
	 *         from bytes
	 * @throws HAWQException
	 *             when byte array doesn't have enough bytes for decimal
	 */
	public static Object bytesToDecimal(byte bytes[], int offset_numeric)
			throws HAWQException
	{
		char[] decimalCharArray = new char[1001];
		byte lead = bytes[offset_numeric];
		int offset, length;
		if ((lead & 0x80) != 0)
		{
			offset = offset_numeric + 1;
			length = (lead & 0x7F) - 1;
		}
		else
		{
			offset = offset_numeric + 4;
			length = Integer.reverseBytes(HAWQConvertUtil.bytesToInt(bytes,
					offset_numeric)) - 4;
		}
		try
		{
			int posInBytes = offset;
			short weightOfFirstDigit = bytesToShort(bytes, posInBytes);
			posInBytes += 2;
			// First two bits mean sign. Rest bits mean display scale
			short sign_dscale = bytesToShort(bytes, posInBytes);
			posInBytes += 2;
			// sign 0x0000 means positive
			// sign 0x4000 means negative
			// sign 0xC000 means not a number(NaN)
			int sign = sign_dscale & 0x0000C000;
			int displayScale = sign_dscale & 0x00003FFF;
			if (sign == 0x0000C000)
			{
				// means 'NaN'
				return "NaN";
			}

			short length_charArray = 0;
			if (sign == 0x4000)
				decimalCharArray[length_charArray++] = '-';
			short dotPos = -1;
			// each two bytes present four digits
			if (weightOfFirstDigit < 0)
			{
				dotPos = length_charArray++;
				decimalCharArray[dotPos] = '.';
				for (int i = -1; i > weightOfFirstDigit; --i)
				{
					decimalCharArray[length_charArray++] = '0';
					decimalCharArray[length_charArray++] = '0';
					decimalCharArray[length_charArray++] = '0';
					decimalCharArray[length_charArray++] = '0';
				}
			}
			for (; posInBytes < offset + length; posInBytes += 2)
			{
				short temp = bytesToShort(bytes, posInBytes);
				decimalCharArray[length_charArray++] = (char) (temp / 1000 + '1' - 1);
				decimalCharArray[length_charArray++] = (char) ((temp / 100) % 10 + '1' - 1);
				decimalCharArray[length_charArray++] = (char) ((temp / 10) % 10 + '1' - 1);
				decimalCharArray[length_charArray++] = (char) (temp % 10 + '1' - 1);
				if (dotPos == -1)
				{
					if (weightOfFirstDigit == 0)
					{
						dotPos = length_charArray++;
						decimalCharArray[dotPos] = '.';
					}
					else if (weightOfFirstDigit > 0)
						--weightOfFirstDigit;
				}
			}
			while (dotPos == -1)
			{
				decimalCharArray[length_charArray++] = '0';
				decimalCharArray[length_charArray++] = '0';
				decimalCharArray[length_charArray++] = '0';
				decimalCharArray[length_charArray++] = '0';
				if (weightOfFirstDigit == 0)
				{
					dotPos = length_charArray++;
					decimalCharArray[dotPos] = '.';
				}
				else if (weightOfFirstDigit > 0)
					--weightOfFirstDigit;
			}
			for (int i = 0; i < displayScale; ++i)
				decimalCharArray[length_charArray++] = '0';
			Object result = new BigDecimal(decimalCharArray, 0, dotPos
					+ displayScale + 1);
			decimalCharArray = null;
			return result;
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException(
					"Cannot convert bytes to decimal with offset is " + offset
							+ " and length is " + length);
		}
	}

	/**
	 * Convert bytes to hawq array
	 * 
	 * @param bytes
	 *            byte array
	 * @param elemtype
	 *            element type in array
	 * @param offset_array
	 *            offset in byte array
	 * @return hawq array converted from bytea array
	 * @throws HAWQException
	 *             when dimension is larger than 3 or there is no enough bytes
	 *             in byte array for an array
	 */
	public static Array bytesToArray(byte bytes[],
			HAWQPrimitiveField.PrimitiveType elemtype, int offset_array)
			throws HAWQException
	{
		byte lead = bytes[offset_array];
		int offset;
		if ((lead & 0x80) != 0)
			offset = offset_array + 1;
		else
			offset = offset_array + 4;

		try
		{
			int posInBytes = offset;
			int dimensions = bytesToInt(bytes, posInBytes);
			if (dimensions > 3)
				throw new HAWQException("Not support array that dimension is "
						+ dimensions);
			posInBytes += 4;
			// TODO: what??
			int what = HAWQConvertUtil.bytesToInt(bytes, posInBytes);
			posInBytes += 4;
			int elemtypeOid = bytesToInt(bytes, posInBytes);
			posInBytes += 4;
			int numOfEachDime[] = new int[dimensions];
			int startOfEachDime[] = new int[dimensions];
			int dataNum = 1;
			for (int j = 0; j < dimensions; ++j)
			{
				numOfEachDime[j] = bytesToInt(bytes, posInBytes);
				posInBytes += 4;
				dataNum *= numOfEachDime[j];
			}
			for (int j = 0; j < dimensions; ++j)
			{
				startOfEachDime[j] = bytesToInt(bytes, posInBytes);
				posInBytes += 4;
			}

			byte[] arrayNullDigit = null;
			boolean[] nullMap = null;
			if (what != 0)
			{
				int numOfInt = (dataNum - 1) / 32 + 1;
				arrayNullDigit = new byte[numOfInt * 4];
				nullMap = new boolean[dataNum];
				for (int i = 0; i < numOfInt * 4; ++i)
					arrayNullDigit[i] = bytes[posInBytes++];

				for (int i = 0; i < dataNum; ++i)
				{
					if (((arrayNullDigit[i / 8] >> (i % 8)) & 1) == 0)
						nullMap[i] = true;
					else
						nullMap[i] = false;
				}
			}

			Object[] datas = new Object[dataNum];
			for (int i = 0; i < dataNum; ++i)
			{
				if (nullMap != null && nullMap[i])
				{
					datas[i] = null;
					continue;
				}

				switch (elemtype)
				{
				case INT4:
					datas[i] = bytesToInt(bytes, posInBytes);
					posInBytes += 4;
					break;
				case INT8:
					datas[i] = bytesToLong(bytes, posInBytes);
					posInBytes += 8;
					break;
				case INT2:
					datas[i] = bytesToShort(bytes, posInBytes);
					posInBytes += 2;
					break;
				case FLOAT4:
					datas[i] = bytesToFloat(bytes, posInBytes);
					posInBytes += 4;
					break;
				case FLOAT8:
					datas[i] = bytesToDouble(bytes, posInBytes);
					posInBytes += 8;
					break;
				case BOOL:
					datas[i] = byteToBoolean(bytes[posInBytes++]);
					break;
				case DATE:
					datas[i] = toDate(bytesToInt(bytes, posInBytes));
					posInBytes += 4;
					break;
				case INTERVAL:
					datas[i] = bytesToInterval(bytes, posInBytes);
					posInBytes += 16;
					break;
				case TIME:
					datas[i] = toTime(bytesToLong(bytes, posInBytes));
					posInBytes += 8;
					break;
				default:
					throw new HAWQException("_"
							+ elemtype.toString().toLowerCase()
							+ " is not supported yet");
				}
			}
			return new HAWQArray(elemtypeOid, elemtype, startOfEachDime,
					numOfEachDime, datas);
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException(
					"Cannot convert bytes to array with offset is " + offset
							+ " and length of bytes is " + bytes.length);
		}
	}

	/**
	 * Convert bytes to path
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset_path
	 *            offset in byte array
	 * @return path value converted from byte array
	 * @throws HAWQException
	 *             when there is not enough bytes in byte array for a path value
	 */
	public static HAWQPath bytesToPath(byte[] bytes, int offset_path)
			throws HAWQException
	{
		byte lead = bytes[offset_path];
		int offset;
		if ((lead & 0x80) != 0)
			offset = offset_path + 1;
		else
			offset = offset_path + 4;

		try
		{
			int posInBytes = offset;
			int numOfPoints = bytesToInt(bytes, posInBytes);
			posInBytes += 4;
			int closed = bytesToInt(bytes, posInBytes);
			posInBytes += 4;
			// TODO: what?
			posInBytes += 4;
			boolean open;
			if (closed == 1)
				open = false;
			else
				open = true;
			ArrayList<HAWQPoint> points = new ArrayList<HAWQPoint>();
			for (int i = 0; i < numOfPoints; ++i)
			{
				double x = bytesToDouble(bytes, posInBytes);
				posInBytes += 8;
				double y = bytesToDouble(bytes, posInBytes);
				posInBytes += 8;
				points.add(new HAWQPoint(x, y));
			}
			return new HAWQPath(open, points);
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException(
					"Cannot convert bytes to path with offset is " + offset
							+ " and length of bytes is " + bytes.length);
		}
	}

	/**
	 * Convert bytes to polygon
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset_polygon
	 *            offset in byte array
	 * @return polygon value converted from byte array
	 * @throws HAWQException
	 *             when there is not enough bytes in byte array for a polygon
	 *             value
	 */
	public static HAWQPolygon bytesToPolygon(byte[] bytes, int offset_polygon)
			throws HAWQException
	{
		byte lead = bytes[offset_polygon];
		int offset;
		if ((lead & 0x80) != 0)
			offset = offset_polygon + 1;
		else
			offset = offset_polygon + 4;

		try
		{
			int posInBytes = offset;
			int numOfPoints = bytesToInt(bytes, posInBytes);
			posInBytes += 4;

			double box_x_1 = bytesToDouble(bytes, posInBytes);
			posInBytes += 8;
			double box_y_1 = bytesToDouble(bytes, posInBytes);
			posInBytes += 8;
			double box_x_2 = bytesToDouble(bytes, posInBytes);
			posInBytes += 8;
			double box_y_2 = bytesToDouble(bytes, posInBytes);
			posInBytes += 8;
			HAWQBox boundbox = new HAWQBox(box_x_1, box_y_1, box_x_2, box_y_2);

			ArrayList<HAWQPoint> points = new ArrayList<HAWQPoint>();
			for (int i = 0; i < numOfPoints; ++i)
			{
				double x = bytesToDouble(bytes, posInBytes);
				posInBytes += 8;
				double y = bytesToDouble(bytes, posInBytes);
				posInBytes += 8;
				points.add(new HAWQPoint(x, y));
			}
			return new HAWQPolygon(points, boundbox);
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException(
					"Cannot convert bytes to polygon with offset is " + offset
							+ " and length of bytes is " + bytes.length);
		}
	}

	/**
	 * Convert bytes to varbit
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset_varbit
	 *            offset in byte array
	 * @return varbit value converted from byte array
	 * @throws HAWQException
	 *             when there is not enough bytes in byte array for a varbit
	 *             value
	 */
	public static HAWQVarbit bytesToVarbit(byte[] bytes, int offset_varbit)
			throws HAWQException
	{
		byte lead = bytes[offset_varbit];
		int offset;
		if ((lead & 0x80) != 0)
			offset = offset_varbit + 1;
		else
			offset = offset_varbit + 4;

		try
		{
			int posInBytes = offset;
			int numOfBits = bytesToInt(bytes, posInBytes);
			posInBytes += 4;
			return new HAWQVarbit(bytes, posInBytes, numOfBits);
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException(
					"Cannot convert bytes to varbit with offset is " + offset
							+ " and length of bytes is " + bytes.length);
		}
	}

	/**
	 * Convert bytes to inet
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset_inet
	 *            offset in byte array
	 * @return inet value converted from byte array
	 * @throws HAWQException
	 *             when there is not enough bytes in byte array for a inet value
	 */
	public static HAWQInet bytesToInet(byte[] bytes, int offset_inet)
			throws HAWQException
	{
		byte lead = bytes[offset_inet];
		int offset;
		if ((lead & 0x80) != 0)
			offset = offset_inet + 1;
		else
			offset = offset_inet + 4;

		try
		{
			int posInBytes = offset;
			byte type = bytes[posInBytes++];
			HAWQInet.InetType inetType = null;
			if (type == 3)
				inetType = HAWQInet.InetType.IPV6;
			else if (type == 2)
				inetType = HAWQInet.InetType.IPV4;
			else
				throw new HAWQException("Wrong type for inet",
						HAWQException.WRONGFILEFORMAT_EXCEPTION);
			short mask = (short) (((short) bytes[posInBytes++]) & 0xFF);
			return new HAWQInet(inetType, bytes, posInBytes, mask);
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException(
					"Cannot convert bytes to inet with offset is " + offset
							+ " and length of bytes is " + bytes.length);
		}
	}

	/**
	 * Convert bytes to cidr
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset_cidr
	 *            offset in byte array
	 * @return cidr value converted from byte array
	 * @throws HAWQException
	 *             when there is not enough bytes in byte array for a cidr value
	 */
	public static HAWQCidr bytesToCidr(byte[] bytes, int offset_cidr)
			throws HAWQException
	{
		byte lead = bytes[offset_cidr];
		int offset;
		if ((lead & 0x80) != 0)
			offset = offset_cidr + 1;
		else
			offset = offset_cidr + 4;

		try
		{
			int posInBytes = offset;
			byte type = bytes[posInBytes++];
			HAWQInet.InetType inetType = null;
			if (type == 3)
				inetType = HAWQInet.InetType.IPV6;
			else if (type == 2)
				inetType = HAWQInet.InetType.IPV4;
			else
				throw new HAWQException("Wrong type for inet",
						HAWQException.WRONGFILEFORMAT_EXCEPTION);
			short mask = (short) (((short) bytes[posInBytes++]) & 0xFF);
			return new HAWQCidr(inetType, bytes, posInBytes, mask);
		}
		catch (ArrayIndexOutOfBoundsException e)
		{
			throw new HAWQException(
					"Cannot convert bytes to cidr with offset is " + offset
							+ " and length of bytes is " + bytes.length);
		}
	}
}
