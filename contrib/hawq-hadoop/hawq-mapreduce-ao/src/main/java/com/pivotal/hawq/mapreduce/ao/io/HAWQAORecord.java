package com.pivotal.hawq.mapreduce.ao.io;

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
import com.pivotal.hawq.mapreduce.schema.HAWQField;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import com.pivotal.hawq.mapreduce.util.HAWQConvertUtil;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Extends {@link HAWQRecord} and realize abstract get/set methods in
 * HAWQRecord. For append only file, object of this class is generated and
 * return back to customer.
 */
public class HAWQAORecord extends HAWQRecord
{
	private String encoding;
	/*
	 * GPSQL-1047
	 * 
	 * If database is working on mac, this value is true;
	 */
	private boolean isMac;

	private HAWQPrimitiveField.PrimitiveType[] schemaType = null;
	private int columnNum;

	private byte[] memtuples = null;
	private int start;
	private int end;
	private int isLargeTup;

	private int[] offsetOfEachColumn;
	private boolean[] nullmap;

	private int numOf8MoreBytes = 0;
	private int[] offsetOf8MoreBytes = null;
	private int numOf4MoreBytes = 0;
	private int[] offsetOf4MoreBytes = null;
	private int numOf2Bytes = 0;
	private int[] offsetOf2Bytes = null;
	private int numOf1Byte = 0;
	private int[] offsetOf1Bytes = null;
	private int numOfOffset = 0;

	private HAWQPrimitiveField.PrimitiveType[] typesOf4MoreBytes = null;
	private HAWQPrimitiveField.PrimitiveType[] typesOf8MoreBytes = null;

	private int nullmapExtraBytes;

	/**
	 * Constructor
	 * 
	 * @param schema
	 *            schema
	 * @param encoding
	 *            encoding in this file
	 * @param version
	 *            version
	 * @throws HAWQException
	 *             when there are unsupproted types in schema
	 */
	public HAWQAORecord(HAWQSchema schema, String encoding, String version)
			throws HAWQException
	{
		super(schema);
		columnNum = schema.getFieldCount();
		schemaType = new HAWQPrimitiveField.PrimitiveType[columnNum];
		offsetOfEachColumn = new int[columnNum];
		nullmap = new boolean[columnNum];
		for (int i = 0; i < columnNum; i++)
		{
			HAWQField field = schema.getField(i + 1);
			if (field.isPrimitive())
				schemaType[i] = field.asPrimitive().getType();
			else
				throw new HAWQException("User define type is not supported yet");
			values[i] = null;
			offsetOfEachColumn[i] = -1;
			nullmap[i] = false;
		}
		nullmapExtraBytes = -1;
		initFromSchema();
		isMac = version.contains("on i386-apple-darwin");
		offsetOf8MoreBytes = new int[numOf8MoreBytes];
		offsetOf4MoreBytes = new int[numOf4MoreBytes + numOfOffset];
		offsetOf2Bytes = new int[numOf2Bytes + numOfOffset];
		offsetOf1Bytes = new int[numOf1Byte];
		this.encoding = encoding;
		encodingMap();
	}

	private void encodingMap()
	{
		if (encoding.equals("ISO_8859_5"))
			encoding = "ISO-8859-5";
		else if (encoding.equals("ISO_8859_6"))
			encoding = "ISO-8859-6";
		else if (encoding.equals("ISO_8859_7"))
			encoding = "ISO-8859-7";
		else if (encoding.equals("ISO_8859_8"))
			encoding = "ISO-8859-8";
		else if (encoding.equals("KOI8R"))
			encoding = "KOI8-R";
		else if (encoding.equals("UTF8"))
			encoding = "UTF-8";
		else if (encoding.equals("WIN1250"))
			encoding = "windows-1250";
		else if (encoding.equals("WIN1251"))
			encoding = "windows-1251";
		else if (encoding.equals("WIN1252"))
			encoding = "windows-1252";
		else if (encoding.equals("WIN1253"))
			encoding = "windows-1253";
		else if (encoding.equals("WIN1254"))
			encoding = "windows-1254";
		else if (encoding.equals("WIN1255"))
			encoding = "windows-1255";
		else if (encoding.equals("WIN1256"))
			encoding = "windows-1256";
		else if (encoding.equals("WIN1257"))
			encoding = "windows-1257";
		else if (encoding.equals("WIN1258"))
			encoding = "windows-1258";
		else if (encoding.equals("JOHAB"))
			encoding = "x-Johab";
		else if (encoding.equals("WIN874"))
			encoding = "x-windows-874";
	}

	@Override
	public boolean getBoolean(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getBoolean(fieldIndex);
	}

	@Override
	public byte getByte(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getByte(fieldIndex);
	}

	@Override
	public byte[] getBytes(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getBytes(fieldIndex);
	}

	@Override
	public double getDouble(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getDouble(fieldIndex);
	}

	@Override
	public float getFloat(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getFloat(fieldIndex);
	}

	@Override
	public int getInt(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getInt(fieldIndex);
	}

	@Override
	public long getLong(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getLong(fieldIndex);
	}

	@Override
	public short getShort(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getShort(fieldIndex);
	}

	@Override
	public String getString(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getString(fieldIndex);
	}

	@Override
	public char getChar(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getChar(fieldIndex);
	}

	@Override
	public boolean isNull(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return (values[fieldIndex - 1] == null);
	}

	@Override
	public Timestamp getTimestamp(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getTimestamp(fieldIndex);
	}

	@Override
	public Time getTime(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getTime(fieldIndex);
	}

	@Override
	public Date getDate(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getDate(fieldIndex);
	}

	@Override
	public BigDecimal getBigDecimal(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getBigDecimal(fieldIndex);
	}

	@Override
	public Array getArray(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getArray(fieldIndex);
	}

	@Override
	public HAWQBox getBox(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getBox(fieldIndex);
	}

	@Override
	public HAWQCircle getCircle(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getCircle(fieldIndex);
	}

	@Override
	public HAWQInterval getInterval(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getInterval(fieldIndex);
	}

	@Override
	public HAWQLseg getLseg(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getLseg(fieldIndex);
	}

	@Override
	public HAWQPath getPath(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getPath(fieldIndex);
	}

	@Override
	public HAWQPoint getPoint(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getPoint(fieldIndex);
	}

	@Override
	public HAWQPolygon getPolygon(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getPolygon(fieldIndex);
	}

	@Override
	public HAWQMacaddr getMacaddr(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getMacaddr(fieldIndex);
	}

	@Override
	public HAWQInet getInet(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getInet(fieldIndex);
	}

	@Override
	public HAWQCidr getCidr(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getCidr(fieldIndex);
	}

	@Override
	public HAWQVarbit getVarbit(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getVarbit(fieldIndex);
	}

	public Object getObject(int fieldIndex) throws HAWQException
	{
		checkFieldIndex(fieldIndex);
		if (values[fieldIndex - 1] == null)
			get(fieldIndex);

		return super.getObject(fieldIndex);
	}

	@Override
	public void reset()
	{
		for (int i = 0; i < columnNum; ++i)
		{
			values[i] = null;
			offsetOfEachColumn[i] = -1;
			nullmap[i] = false;
		}
		nullmapExtraBytes = -1;
	}

	public void setMemtuples(byte[] memtuples, int start, int end)
	{
		this.memtuples = memtuples;
		this.start = start;
		this.end = end;
	}

	private void initFromSchema() throws HAWQException
	{
		for (int i = 0; i < columnNum; ++i)
		{
			HAWQField field = super.getSchema().getField(i + 1);
			if (field.isArray())
			{
				++numOfOffset;
				continue;
			}

			switch (schemaType[i])
			{
			case FLOAT8:
			case INT8:
			case TIME:
			case TIMETZ:
			case TIMESTAMP:
			case TIMESTAMPTZ:
			case LSEG:
			case BOX:
			case CIRCLE:
			case INTERVAL:
			case POINT:
				++numOf8MoreBytes;
				break;
			case MACADDR:
			case INT4:
			case FLOAT4:
			case DATE:
				++numOf4MoreBytes;
				break;
			case INT2:
				++numOf2Bytes;
				break;
			case BOOL:
				++numOf1Byte;
				break;
			case TEXT:
			case BPCHAR:
			case VARCHAR:
			case INET:
			case CIDR:
			case XML:
			case NUMERIC:
			case BYTEA:
			case PATH:
			case POLYGON:
			case BIT:
			case VARBIT:
				++numOfOffset;
				break;
			default:
				break;
			}
		}
		typesOf8MoreBytes = new HAWQPrimitiveField.PrimitiveType[numOf8MoreBytes];
		typesOf4MoreBytes = new HAWQPrimitiveField.PrimitiveType[numOf4MoreBytes];

		int posIn8ByteTypes = 0, posIn4ByteTypes = 0;
		for (int i = 0; i < columnNum; ++i)
		{
			HAWQField field = super.getSchema().getField(i + 1);
			if (field.isArray())
				continue;

			switch (schemaType[i])
			{
			case FLOAT8:
			case INT8:
			case TIME:
			case TIMETZ:
			case TIMESTAMP:
			case TIMESTAMPTZ:
			case LSEG:
			case BOX:
			case CIRCLE:
			case INTERVAL:
			case POINT:
				typesOf8MoreBytes[posIn8ByteTypes++] = schemaType[i];
				break;
			case MACADDR:
			case INT4:
			case FLOAT4:
			case DATE:
				typesOf4MoreBytes[posIn4ByteTypes++] = schemaType[i];
				break;
			default:
				break;
			}
		}
	}

	private void get(int fieldIndex) throws HAWQException
	{
		if (offsetOfEachColumn[0] == -1)
			initOffset();
		int i = fieldIndex - 1;
		int offset = offsetOfEachColumn[i];
		if (offset == 0)
		{
			values[i] = null;
			return;
		}

		HAWQField field = schema.getField(i + 1);
		if (field.isArray())
		{
			int offset_array;
			if (isLargeTup != 0)
				offset_array = HAWQConvertUtil.bytesToInt(memtuples, offset);
			else
				offset_array = ((int) HAWQConvertUtil.bytesToShort(memtuples,
						offset)) & 0x0000FFFF;
			offset_array += start;
			offset_array += nullmapExtraBytes;
			values[i] = HAWQConvertUtil.bytesToArray(memtuples, schemaType[i],
					offset_array);
			return;
		}

		switch (schemaType[i])
		{
		case INT4:
			values[i] = HAWQConvertUtil.bytesToInt(memtuples, offset);
			break;
		case INT8:
			values[i] = HAWQConvertUtil.bytesToLong(memtuples, offset);
			break;
		case FLOAT4:
			values[i] = HAWQConvertUtil.bytesToFloat(memtuples, offset);
			break;
		case FLOAT8:
			values[i] = HAWQConvertUtil.bytesToDouble(memtuples, offset);
			break;
		case BOOL:
			values[i] = HAWQConvertUtil.byteToBoolean(memtuples[offset]);
			break;
		case INT2:
			values[i] = HAWQConvertUtil.bytesToShort(memtuples, offset);
			break;
		case TEXT:
		case BPCHAR:
		case VARCHAR:
		case XML:
			int offset_varchar;
			if (isLargeTup != 0)
				offset_varchar = HAWQConvertUtil.bytesToInt(memtuples, offset);
			else
				offset_varchar = ((int) HAWQConvertUtil.bytesToShort(memtuples,
						offset)) & 0x0000FFFF;
			offset_varchar += start;
			offset_varchar += nullmapExtraBytes;
			byte lead_String = memtuples[offset_varchar];
			int offset_String,
			length_String;
			if ((lead_String & 0x80) != 0)
			{
				offset_String = offset_varchar + 1;
				length_String = (lead_String & 0x7F) - 1;
			}
			else
			{
				offset_String = offset_varchar + 4;
				length_String = Integer.reverseBytes(HAWQConvertUtil
						.bytesToInt(memtuples, offset_varchar)) - 4;
			}
			try
			{
				values[i] = new String(memtuples, offset_String, length_String,
						encoding);
			}
			catch (UnsupportedEncodingException e)
			{
				throw new HAWQException("Encoding " + encoding
						+ " is not supported yet");
			}
			catch (StringIndexOutOfBoundsException e)
			{
				throw new HAWQException(
						"Cannot convert bytes to string with offset is "
								+ offset_String + " and length is "
								+ length_String);
			}
			break;
		case INET:
			int offset_inet;
			if (isLargeTup != 0)
				offset_inet = HAWQConvertUtil.bytesToInt(memtuples, offset);
			else
				offset_inet = ((int) HAWQConvertUtil.bytesToShort(memtuples,
						offset)) & 0x0000FFFF;
			offset_inet += start;
			offset_inet += nullmapExtraBytes;
			values[i] = HAWQConvertUtil.bytesToInet(memtuples, offset_inet);
			break;
		case CIDR:
			int offset_cidr;
			if (isLargeTup != 0)
				offset_cidr = HAWQConvertUtil.bytesToInt(memtuples, offset);
			else
				offset_cidr = ((int) HAWQConvertUtil.bytesToShort(memtuples,
						offset)) & 0x0000FFFF;
			offset_cidr += start;
			offset_cidr += nullmapExtraBytes;
			values[i] = HAWQConvertUtil.bytesToCidr(memtuples, offset_cidr);
			break;
		case NUMERIC:
			int offset_numeric;
			if (isLargeTup != 0)
				offset_numeric = HAWQConvertUtil.bytesToInt(memtuples, offset);
			else
				offset_numeric = ((int) HAWQConvertUtil.bytesToShort(memtuples,
						offset)) & 0x0000FFFF;
			offset_numeric += start;
			offset_numeric += nullmapExtraBytes;
			values[i] = HAWQConvertUtil
					.bytesToDecimal(memtuples, offset_numeric);
			break;
		case TIME:
			values[i] = HAWQConvertUtil.toTime(
					HAWQConvertUtil.bytesToLong(memtuples, offset));
			break;
		case TIMETZ:
			values[i] = HAWQConvertUtil.toTimeTz(memtuples, offset);
			break;
		case TIMESTAMP:
			values[i] = HAWQConvertUtil.toTimestamp(
					HAWQConvertUtil.bytesToLong(memtuples, offset), false);
			break;
		case TIMESTAMPTZ:
			values[i] = HAWQConvertUtil.toTimestamp(
					HAWQConvertUtil.bytesToLong(memtuples, offset), true);
			break;
		case DATE:
			values[i] = HAWQConvertUtil.toDate(
					HAWQConvertUtil.bytesToInt(memtuples, offset));
			break;
		case BYTEA:
			int offset_varbinary;
			if (isLargeTup != 0)
				offset_varbinary = HAWQConvertUtil
						.bytesToInt(memtuples, offset);
			else
				offset_varbinary = ((int) HAWQConvertUtil.bytesToShort(
						memtuples, offset)) & 0x0000FFFF;
			offset_varbinary += start;
			offset_varbinary += nullmapExtraBytes;
			byte lead_Bytes = memtuples[offset_varbinary];
			int offset_Bytes,
			length_Bytes;
			if ((lead_Bytes & 0x80) != 0)
			{
				offset_Bytes = offset_varbinary + 1;
				length_Bytes = (lead_Bytes & 0x7F) - 1;
			}
			else
			{
				offset_Bytes = offset_varbinary + 4;
				length_Bytes = Integer.reverseBytes(HAWQConvertUtil.bytesToInt(
						memtuples, offset_varbinary)) - 4;
			}

			byte[] bytes = new byte[length_Bytes];
			System.arraycopy(memtuples, offset_Bytes, bytes, 0, length_Bytes);
			values[i] = bytes;
			break;
		case INTERVAL:
			values[fieldIndex - 1] = HAWQConvertUtil.bytesToInterval(memtuples,
					offset);
			break;
		case POINT:
			double x = HAWQConvertUtil.bytesToDouble(memtuples, offset);
			double y = HAWQConvertUtil.bytesToDouble(memtuples, offset + 8);
			values[fieldIndex - 1] = new HAWQPoint(x, y);
			break;
		case LSEG:
			double lseg_x_1 = HAWQConvertUtil.bytesToDouble(memtuples, offset);
			double lseg_y_1 = HAWQConvertUtil.bytesToDouble(memtuples,
					offset + 8);
			double lseg_x_2 = HAWQConvertUtil.bytesToDouble(memtuples,
					offset + 16);
			double lseg_y_2 = HAWQConvertUtil.bytesToDouble(memtuples,
					offset + 24);
			values[fieldIndex - 1] = new HAWQLseg(lseg_x_1, lseg_y_1, lseg_x_2,
					lseg_y_2);
			break;
		case BOX:
			double box_x_1 = HAWQConvertUtil.bytesToDouble(memtuples, offset);
			double box_y_1 = HAWQConvertUtil.bytesToDouble(memtuples,
					offset + 8);
			double box_x_2 = HAWQConvertUtil.bytesToDouble(memtuples,
					offset + 16);
			double box_y_2 = HAWQConvertUtil.bytesToDouble(memtuples,
					offset + 24);
			values[fieldIndex - 1] = new HAWQBox(box_x_1, box_y_1, box_x_2,
					box_y_2);
			break;
		case CIRCLE:
			double centerX = HAWQConvertUtil.bytesToDouble(memtuples, offset);
			double centerY = HAWQConvertUtil.bytesToDouble(memtuples,
					offset + 8);
			double r = HAWQConvertUtil.bytesToDouble(memtuples, offset + 16);
			values[fieldIndex - 1] = new HAWQCircle(centerX, centerY, r);
			break;
		case PATH:
			int offset_path;
			if (isLargeTup != 0)
				offset_path = HAWQConvertUtil.bytesToInt(memtuples, offset);
			else
				offset_path = ((int) HAWQConvertUtil.bytesToShort(memtuples,
						offset)) & 0x0000FFFF;
			offset_path += start;
			offset_path += nullmapExtraBytes;
			values[fieldIndex - 1] = HAWQConvertUtil.bytesToPath(memtuples,
					offset_path);
			break;
		case POLYGON:
			int offset_polygon;
			if (isLargeTup != 0)
				offset_polygon = HAWQConvertUtil.bytesToInt(memtuples, offset);
			else
				offset_polygon = ((int) HAWQConvertUtil.bytesToShort(memtuples,
						offset)) & 0x0000FFFF;
			offset_polygon += start;
			offset_polygon += nullmapExtraBytes;
			values[fieldIndex - 1] = HAWQConvertUtil.bytesToPolygon(memtuples,
					offset_polygon);
			break;
		case BIT:
		case VARBIT:
			int offser_varbit;
			if (isLargeTup != 0)
				offser_varbit = HAWQConvertUtil.bytesToInt(memtuples, offset);
			else
				offser_varbit = ((int) HAWQConvertUtil.bytesToShort(memtuples,
						offset)) & 0x0000FFFF;
			offser_varbit += start;
			offser_varbit += nullmapExtraBytes;
			values[fieldIndex - 1] = HAWQConvertUtil.bytesToVarbit(memtuples,
					offser_varbit);
			break;
		case MACADDR:
			values[fieldIndex - 1] = new HAWQMacaddr(memtuples, offset);
			break;
		default:
			break;
		}
	}

	private void initOffset() throws HAWQException
	{
		int posInMemTuples = start;
		int lengthMemTuple = HAWQConvertUtil.bytesToInt(memtuples,
				posInMemTuples);
		posInMemTuples += 4;
		if ((lengthMemTuple & 0x80000000) == 0)
			throw new HAWQException("Fail to get length memtuple",
					HAWQException.WRONGFILEFORMAT_EXCEPTION);
		int tupleBytesNum = lengthMemTuple & 0x7FFFFFF8;
		if (tupleBytesNum != end - start + 1)
			throw new HAWQException("Different size of data tuples: "
					+ tupleBytesNum + " and " + (end - start + 1),
					HAWQException.WRONGFILEFORMAT_EXCEPTION);
		int hasExternal = lengthMemTuple & 4;
		if (hasExternal != 0)
			throw new HAWQException("hasExternal bit of length tuple is not 0",
					HAWQException.WRONGFILEFORMAT_EXCEPTION);

		int hasNull = lengthMemTuple & 1;
		int numOfNullmapBytes = 0;
		if (hasNull != 0)
		{
			numOfNullmapBytes = ((columnNum - 1) / 32 + 1) * 4;
			byte[] numDigit = new byte[numOfNullmapBytes];
			for (int i = 0; i < numDigit.length; ++i)
				numDigit[i] = memtuples[posInMemTuples++];

			for (int i = 0; i < columnNum; ++i)
			{
				if (((numDigit[i / 8] >> (i % 8)) & 1) != 0)
					nullmap[i] = true;
				else
					nullmap[i] = false;
			}
		}

		isLargeTup = lengthMemTuple & 2;
		int tempNumOf4Bytes, tempNumOf2Bytes;
		if (isLargeTup != 0)
		{
			// offsets are 4 bytes
			tempNumOf4Bytes = numOf4MoreBytes + numOfOffset;
			tempNumOf2Bytes = numOf2Bytes;
		}
		else
		{
			// offsets are 2 bytes
			tempNumOf4Bytes = numOf4MoreBytes;
			tempNumOf2Bytes = numOf2Bytes + numOfOffset;
		}

		if (numOf8MoreBytes != 0)
		{
			/*
			 * GPSQL-990
			 * 
			 * nullmapExtraBytes should be calculated even though all data that
			 * 8 more bytes is null
			 */
			int length = 4 + numOfNullmapBytes;
			if (length == 8)
				// Null map need 4 bytes, size of null map and length
				// memtuple is 8 and no need to fill up
				nullmapExtraBytes = 0;
			else if (length % 8 == 0)
				// Null map is 12, 20, etc. no need to fill up. But
				// there is some bytes more for null map
				nullmapExtraBytes = numOfNullmapBytes - 4;
			else
			{
				// null map is 0, 8, etc. need to fill up 4 bytes, so we
				// skip this 4 bytes
				posInMemTuples += 4;
				nullmapExtraBytes = numOfNullmapBytes;
			}
		}
		else
		{
			/*
			 * GPSQL-941
			 * 
			 * If there is no 8 more bytes and there is null value in tuple, the
			 * nullmapExtraBytes is equal to numOfNullmapBytes rather than 0
			 */
			nullmapExtraBytes = numOfNullmapBytes;
		}

		int pos = 0;
		for (int i = 0; i < numOf8MoreBytes; ++i)
		{
			if (hasNull != 0 && nullmap[pos])
			{
				offsetOf8MoreBytes[i] = 0;
			}
			else
			{
				offsetOf8MoreBytes[i] = posInMemTuples;
				switch (typesOf8MoreBytes[i])
				{
				case FLOAT8:
				case INT8:
				case TIME:
				case TIMESTAMP:
				case TIMESTAMPTZ:
					posInMemTuples += 8;
					break;
				case TIMETZ:
					/*
					 * GPSQL-1047
					 * 
					 * For 64-bit os and db, timetz is aligned to 16 bits.
					 */
					if (isMac)
						posInMemTuples += 12;
					else
					{
						posInMemTuples += 16;
						if (i == numOf8MoreBytes - 1)
							posInMemTuples -= 4;
					}
					break;
				case LSEG:
				case BOX:
					posInMemTuples += 32;
					break;
				case CIRCLE:
					posInMemTuples += 24;
					break;
				case INTERVAL:
				case POINT:
					posInMemTuples += 16;
					break;
				default:
					break;
				}
			}
			++pos;
		}
		for (int i = 0; i < tempNumOf4Bytes; ++i)
		{
			if (hasNull != 0 && nullmap[pos])
			{
				offsetOf4MoreBytes[i] = 0;
			}
			else
			{
				offsetOf4MoreBytes[i] = posInMemTuples;
				switch (typesOf4MoreBytes[i])
				{
				case MACADDR:
					posInMemTuples += 8;
					if (i == tempNumOf4Bytes - 1)
						posInMemTuples -= 2;
					break;
				case INT4:
				case FLOAT4:
				case DATE:
					posInMemTuples += 4;
					break;
				default:
					break;
				}
			}
			++pos;
		}
		for (int i = 0; i < tempNumOf2Bytes; ++i)
		{
			if (hasNull != 0 && nullmap[pos])
			{
				offsetOf2Bytes[i] = 0;
			}
			else
			{
				offsetOf2Bytes[i] = posInMemTuples;
				posInMemTuples += 2;
			}
			++pos;
		}
		for (int i = 0; i < numOf1Byte; ++i)
		{
			if (hasNull != 0 && nullmap[pos])
			{
				offsetOf1Bytes[i] = 0;
			}
			else
			{
				offsetOf1Bytes[i] = posInMemTuples++;
			}
			++pos;
		}

		int posIn8Byte = 0, posIn4Byte = 0, posIn2Byte = 0, posIn1Byte = 0;
		for (int i = 0; i < columnNum; ++i)
		{
			HAWQField field = super.getSchema().getField(i + 1);
			if (field.isArray())
			{
				if (isLargeTup != 0)
					offsetOfEachColumn[i] = offsetOf4MoreBytes[posIn4Byte++];
				else
					offsetOfEachColumn[i] = offsetOf2Bytes[posIn2Byte++];
				continue;
			}

			switch (schemaType[i])
			{
			case INT8:
			case FLOAT8:
			case TIME:
			case TIMETZ:
			case TIMESTAMP:
			case TIMESTAMPTZ:
			case INTERVAL:
			case POINT:
			case LSEG:
			case BOX:
			case CIRCLE:
				offsetOfEachColumn[i] = offsetOf8MoreBytes[posIn8Byte++];
				break;
			case MACADDR:
			case INT4:
			case FLOAT4:
			case DATE:
				offsetOfEachColumn[i] = offsetOf4MoreBytes[posIn4Byte++];
				break;
			case INT2:
				offsetOfEachColumn[i] = offsetOf2Bytes[posIn2Byte++];
				break;
			case BOOL:
				offsetOfEachColumn[i] = offsetOf1Bytes[posIn1Byte++];
				break;
			case PATH:
			case POLYGON:
			case VARCHAR:
			case INET:
			case CIDR:
			case XML:
			case BPCHAR:
			case TEXT:
			case BYTEA:
			case NUMERIC:
			case BIT:
			case VARBIT:
				if (isLargeTup != 0)
					offsetOfEachColumn[i] = offsetOf4MoreBytes[posIn4Byte++];
				else
					offsetOfEachColumn[i] = offsetOf2Bytes[posIn2Byte++];
				break;
			default:
				break;
			}
		}
	}
}
