package com.pivotal.hawq.mapreduce.util;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.datatype.HAWQArray;
import com.pivotal.hawq.mapreduce.datatype.HAWQBox;
import com.pivotal.hawq.mapreduce.datatype.HAWQCidr;
import com.pivotal.hawq.mapreduce.datatype.HAWQDate;
import com.pivotal.hawq.mapreduce.datatype.HAWQInet;
import com.pivotal.hawq.mapreduce.datatype.HAWQInterval;
import com.pivotal.hawq.mapreduce.datatype.HAWQPath;
import com.pivotal.hawq.mapreduce.datatype.HAWQPoint;
import com.pivotal.hawq.mapreduce.datatype.HAWQPolygon;
import com.pivotal.hawq.mapreduce.datatype.HAWQTime;
import com.pivotal.hawq.mapreduce.datatype.HAWQTimestamp;
import com.pivotal.hawq.mapreduce.datatype.HAWQVarbit;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * This class is an util class and supply static methods for type converting.
 */
public abstract class HAWQConvertUtil
{
	public static char[] decimalCharArray = null;
	private static long microsecondsPerDay = 86400000000l;

	/**
	 * Convert long to timestamp
	 * 
	 * @param microseconds
	 *            microseconds
	 * @return timestamp converted from micorseconds
	 */
	public static Timestamp longToTimestamp(long microseconds)
	{
		long days = microseconds / microsecondsPerDay;
		HAWQDate date = (HAWQDate) integerToDate((int) days);
		return new HAWQTimestamp(date, microseconds % microsecondsPerDay,
				date.isBC());
	}

	/**
	 * Convert long to timestamp with time zone
	 * 
	 * @param microseconds
	 *            microseconds
	 * @return timestamp with time zone converted from micorseconds
	 */
	public static Timestamp longToTimestampTz(long microseconds)
	{
		long days = microseconds / microsecondsPerDay;
		HAWQDate date = (HAWQDate) integerToDate((int) days);
		return new HAWQTimestamp(date, microseconds % microsecondsPerDay,
				date.isBC(), TimeZone.getDefault().getRawOffset());
	}

	/**
	 * Convert long to time
	 * 
	 * @param microseconds
	 *            microseconds
	 * @return time converted from microseconds
	 */
	public static Time longToTime(long microseconds)
	{
		long milliseconds = microseconds / 1000;
		int microsecond = (int) (microseconds % 1000);
		return new HAWQTime(milliseconds, microsecond);
	}

	/**
	 * Convert 12 bytes to time with time zone
	 * 
	 * @param bytes
	 *            byte array
	 * @param offset
	 *            offset in byte array
	 * @return time with time zone converted from bytes
	 * @throws HAWQException
	 *             when there is no enough bytes in byte array
	 */
	public static Time bytesToTimeTz(byte[] bytes, int offset)
			throws HAWQException
	{
		long microseconds = bytesToLong(bytes, offset);
		int zoneSecondOffset = bytesToInt(bytes, offset + 8);
		long milliseconds = microseconds / 1000;
		int microsecond = (int) (microseconds % 1000);
		return new HAWQTime(milliseconds, microsecond, zoneSecondOffset);
	}

	/**
	 * Convert integer to date
	 * 
	 * @param days
	 *            days
	 * @return date converted from days
	 */
	public static Date integerToDate(int days)
	{
		int julian;
		int quad;
		int extra;
		int y;

		julian = days + HAWQDate.date2j(2000, 1, 1);
		julian += 32044;
		quad = julian / 146097;
		extra = (julian - quad * 146097) * 4 + 3;
		julian += 60 + quad * 3 + extra / 146097;
		quad = julian / 1461;
		julian -= quad * 1461;
		y = julian * 4 / 1461;
		julian = ((y != 0) ? (julian + 305) % 365 : (julian + 306) % 366) + 123;
		y += quad * 4;
		int year = y - 4800;
		quad = julian * 2141 / 65536;
		int day = julian - 7834 * quad / 256;
		int month = (quad + 10) % 12 + 1;

		boolean isBC = false;
		if (year < 0)
		{
			year = -year + 1;
			isBC = true;
		}
		GregorianCalendar calendar = new GregorianCalendar(year, month - 1, day);
		return new HAWQDate(calendar.getTimeInMillis(), isBC);
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
			throws HAWQException
	{
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
		if (decimalCharArray == null)
			decimalCharArray = new char[1001];
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
			return new BigDecimal(HAWQConvertUtil.decimalCharArray, 0, dotPos
					+ displayScale + 1);
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
					datas[i] = integerToDate(bytesToInt(bytes, posInBytes));
					posInBytes += 4;
					break;
				case INTERVAL:
					datas[i] = bytesToInterval(bytes, posInBytes);
					posInBytes += 16;
					break;
				case TIME:
					datas[i] = longToTime(bytesToLong(bytes, posInBytes));
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
	public static HAWQInet bytesToCidr(byte[] bytes, int offset_cidr)
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