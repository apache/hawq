package com.pivotal.hawq.mapreduce.datatype;

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

/**
 * Store value of varbit/bit in database
 */
public class HAWQVarbit
{
	private byte[] bytes = null;
	private byte[] bytesFromString = null;
	private int numOfBits = -1;

	/**
	 * Initialize a varbit from string
	 * 
	 * @param value
	 *            the value that varbit init from. Should be like this:
	 *            10100010001001111
	 * @throws HAWQException
	 *             when this value is not correct for varbit
	 */
	public HAWQVarbit(String value) throws HAWQException
	{
		bytesFromString = new byte[value.length()];
		for (int i = 0; i < value.length(); i++)
		{
			char c = value.charAt(i);
			if (c != '0' && c != '1')
				throw new HAWQException("'" + c + "'"
						+ " is not a valid binary digit");
			bytesFromString[i] = Byte.parseByte(String.valueOf(c));
		}
	}

	/**
	 * Initialize a varbit from byte array
	 * 
	 * @param bytes
	 *            the byte array that varbit init from
	 * @param numOfBits
	 *            how may bit in this varbit
	 */
	public HAWQVarbit(byte[] bytes, int numOfBits)
	{
		this(bytes, 0, numOfBits);
	}

	/**
	 * 
	 * Initialize a varbit from byte array
	 * 
	 * @param bytes
	 *            the byte array that varbit init from
	 * @param offset
	 *            offset in this byte array
	 * @param numOfBits
	 *            how may bit in this varbit
	 */
	public HAWQVarbit(byte[] bytes, int offset, int numOfBits)
	{
		int length = (numOfBits - 1) / 8 + 1;
		this.bytes = new byte[length];
		for (int i = 0; i < length; i++)
			this.bytes[i] = bytes[offset + i];
		this.numOfBits = numOfBits;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQVarbit)
			return toString().equals(obj.toString());

		return false;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		int arraySize;
		if (bytes != null)
		{
			int outBitNum = 0;
			arraySize = bytes.length;
			for (int i = 0; i < arraySize; i++)
			{
				for (int j = 7; j >= 0; j--)
				{
					buffer.append((bytes[i] >> j) & 1);
					++outBitNum;
					if (outBitNum == numOfBits)
						break;
				}
			}
		}
		else
		{
			arraySize = bytesFromString.length;
			for (int i = 0; i < arraySize; i++)
				buffer.append(bytesFromString[i]);
		}
		return buffer.toString();
	}
}
