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


import java.util.Arrays;

/**
 * Store value of macaddr in database
 */
public class HAWQMacaddr
{
	private byte[] bytes = new byte[6];

	/**
	 * Initialize a macaddr from byte array
	 * 
	 * @param bytes
	 *            the byte array that macaddr init from
	 */
	public HAWQMacaddr(byte[] bytes)
	{
		this(bytes, 0);
	}

	/**
	 * Initialize a macaddr from byte array
	 * 
	 * @param bytes
	 *            the byte array that macaddr init from
	 * @param offset
	 *            offset in bytes
	 */
	public HAWQMacaddr(byte[] bytes, int offset)
	{
		if (bytes.length - offset >= 6)
			System.arraycopy(bytes, offset, this.bytes, 0, 6);
		else
		{
			System.arraycopy(bytes, offset, this.bytes, 0, bytes.length
					- offset);
			for (int i = bytes.length - offset; i < 6; i++)
				bytes[i] = 0;
		}
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj instanceof HAWQMacaddr)
			return Arrays.equals(this.bytes, ((HAWQMacaddr) obj).bytes);
		return false;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < 6; ++i)
		{
			if (bytes[i] <= 0x0F && bytes[i] >= 0)
				buffer.append('0');
			buffer.append(Integer.toHexString(((int) bytes[i]) & 0xFF));
			if (i != 5)
				buffer.append(':');
		}
		return buffer.toString();
	}
}
