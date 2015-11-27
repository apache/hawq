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
 * Store value of inet in database
 */
public class HAWQInet
{
	public static enum InetType
	{
		IPV4, IPV6
	}

	private short mask;
	private int[] ipv6_value;
	private int[] ipv4_value;
	private InetType inetType;
	private String value = null;

	/**
	 * Initialize an inet from string
	 * 
	 * @param value
	 *            the value that inet init from
	 * @throws HAWQException
	 *             when this value is not correct for inet
	 */
	public HAWQInet(String value) throws HAWQException
	{
		try
		{
			int offset = value.indexOf('.');
			int maskOffset = value.indexOf('/');
			String valueWithoutMask = value;
			if (maskOffset != -1)
			{
				mask = Short.parseShort(value.substring(maskOffset + 1));
				valueWithoutMask = value.substring(0, maskOffset);
			}

			if (offset != -1)
			{
				if (maskOffset == -1)
					mask = 32;
				inetType = InetType.IPV4;
				ipv4_value = new int[4];
				int dot1 = valueWithoutMask.indexOf('.');
				int dot2 = valueWithoutMask.indexOf('.', dot1 + 1);
				int dot3 = valueWithoutMask.indexOf('.', dot2 + 1);
				ipv4_value[0] = Integer.parseInt(valueWithoutMask.substring(0,
						dot1));
				ipv4_value[1] = Integer.parseInt(valueWithoutMask.substring(
						dot1 + 1, dot2));
				ipv4_value[2] = Integer.parseInt(valueWithoutMask.substring(
						dot2 + 1, dot3));
				ipv4_value[3] = Integer.parseInt(valueWithoutMask
						.substring(dot3 + 1));

				if (mask == 32)
					this.value = valueWithoutMask;
				else
					this.value = value;
			}
			else
			{
				if (maskOffset == -1)
					mask = 128;
				inetType = InetType.IPV6;
				ipv6_value = new int[8];
				for (int i = 0; i < 8; ++i)
					ipv6_value[i] = 0;

				int indexOfMiddleZero = valueWithoutMask.indexOf("::");
				if (indexOfMiddleZero == -1)
				{
					String[] values = valueWithoutMask.split(":");
					for (int i = 0; i < 8; ++i)
						ipv6_value[i] = Integer.parseInt(values[i], 16);
				}
				else
				{
					String[] valueFirstPart = valueWithoutMask.substring(0,
							indexOfMiddleZero).split(":");
					String[] valueSecondPart = valueWithoutMask.substring(
							indexOfMiddleZero + 2).split(":");
					if (valueFirstPart.length != 1
							|| !valueFirstPart[0].equals(""))
						for (int i = 0; i < valueFirstPart.length; ++i)
							ipv6_value[i] = Integer.parseInt(valueFirstPart[i],
									16);
					if (valueSecondPart.length != 1
							|| !valueSecondPart[0].equals(""))
						for (int i = 0; i < valueSecondPart.length; ++i)
							ipv6_value[7 - i] = Integer.parseInt(
									valueSecondPart[i], 16);
				}
				if (mask == 128)
					this.value = valueWithoutMask;
				else
					this.value = value;
			}
		}
		catch (Exception e)
		{
			throw new HAWQException("Cannot convert " + value + " to HAWQInet");
		}
	}

	/**
	 * Initialize an inet from byte array
	 * 
	 * @param inetType
	 *            ipv4 or ipv6
	 * @param bytes
	 *            byte array that inet init from
	 * @param mask
	 *            net mask
	 * @throws HAWQException
	 *             length of bytes is not enough to init inet or inetType is
	 *             invalid
	 */
	public HAWQInet(InetType inetType, byte[] bytes, short mask)
			throws HAWQException
	{
		this(inetType, bytes, 0, mask);
	}

	/**
	 * Initialize an inet from byte array
	 * 
	 * @param inetType
	 *            ipv4 or ipv6
	 * @param bytes
	 *            byte array that inet init from
	 * @param offset
	 *            offset in byte array
	 * @param mask
	 *            net mask
	 * @throws HAWQException
	 *             length of bytes is not enough to init inet or inetType is
	 *             invalid
	 */
	public HAWQInet(InetType inetType, byte[] bytes, int offset, short mask)
			throws HAWQException
	{
		this.inetType = inetType;
		this.mask = mask;
		switch (this.inetType)
		{
		case IPV6:
			ipv6_value = new int[8];
			try
			{
				for (int i = 0; i < 8; ++i)
				{
					ipv6_value[i] = (((int) bytes[offset + 2 * i] & 0xFF) << 8)
							| (((int) bytes[offset + 2 * i + 1]) & 0xFF);
				}
			}
			catch (ArrayIndexOutOfBoundsException e)
			{
				throw new HAWQException("Need at least 16 bytes: offset is "
						+ offset + " while length of bytes is " + bytes.length);
			}
			break;
		case IPV4:
			ipv4_value = new int[4];
			try
			{
				ipv4_value[0] = (int) bytes[offset] & 0xFF;
				ipv4_value[1] = (int) bytes[offset + 1] & 0xFF;
				ipv4_value[2] = (int) bytes[offset + 2] & 0xFF;
				ipv4_value[3] = (int) bytes[offset + 3] & 0xFF;
			}
			catch (ArrayIndexOutOfBoundsException e)
			{
				throw new HAWQException("Need at least 4 bytes: offset is "
						+ offset + " while length of bytes is " + bytes.length);
			}
			break;
		default:
			throw new HAWQException("Wrong inet type");
		}

	}

	/**
	 * Get type of this inet(ipv4/ipv6)
	 * 
	 * @return type of this inet
	 */
	public InetType getInetType()
	{
		return inetType;
	}

	/**
	 * Get net mask of this inet
	 * 
	 * @return net mask
	 */
	public short getMask()
	{
		return mask;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQInet)
		{
			HAWQInet other = (HAWQInet) obj;
			if (inetType != other.getInetType())
				return false;
			if (mask != other.getMask())
				return false;
			if (!toString().equals(other.toString()))
				return false;
			return true;
		}
		return false;
	}

	@Override
	public String toString()
	{
		if (value != null)
			return value;

		StringBuffer buffer = new StringBuffer();
		switch (inetType)
		{
		case IPV6:
			boolean hasContinuousZero = false;
			for (int i = 0; i < 8; ++i)
			{
				buffer.append(Integer.toHexString(ipv6_value[i]));
				if (i != 7)
				{
					buffer.append(':');
					if (ipv6_value[i] == 0 && ipv6_value[i + 1] == 0)
						hasContinuousZero = true;
				}
			}
			if (mask != 128)
				buffer.append('/').append(mask);

			if (hasContinuousZero)
			{
				StringBuffer temp = new StringBuffer();
				int offset = -1, length = -1;
				for (int i = 7; i >= 2; --i)
				{
					temp.delete(0, buffer.length());
					for (int j = 0; j < i; ++j)
					{
						if (j != 0)
							temp.append(':');
						temp.append('0');
					}
					offset = buffer.indexOf(temp.toString());
					if (offset != -1)
					{
						length = temp.length();
						break;
					}
				}
				if (length != 13)
					buffer.replace(offset, offset + length, "");
				else
					buffer.replace(offset, offset + length, ":");
			}
			break;
		case IPV4:
			buffer.append(ipv4_value[0]).append('.').append(ipv4_value[1])
					.append('.').append(ipv4_value[2]).append('.')
					.append(ipv4_value[3]);
			if (mask != 32)
				buffer.append('/').append(mask);
			break;
		default:
			return null;
		}
		return buffer.toString();
	}
}
