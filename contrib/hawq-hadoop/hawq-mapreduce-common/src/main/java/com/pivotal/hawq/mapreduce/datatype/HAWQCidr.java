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
 * Store value of cidr in database
 */
public class HAWQCidr extends HAWQInet
{
	/**
	 * Initialize an cidr from byte array
	 * 
	 * @param inetType
	 *            ipv4 or ipv6
	 * @param bytes
	 *            byte array that cidr init from
	 * @param mask
	 *            net mask
	 * @throws HAWQException
	 *             length of bytes is not enough to init cidr or inetType is
	 *             invalid
	 */
	public HAWQCidr(InetType inetType, byte[] bytes, short mask)
			throws HAWQException
	{
		this(inetType, bytes, 0, mask);
	}

	/**
	 * Initialize an cidr from byte array
	 * 
	 * @param inetType
	 *            ipv4 or ipv6
	 * @param bytes
	 *            byte array that cidr init from
	 * @param offset
	 *            offset in byte array
	 * @param mask
	 *            net mask
	 * @throws HAWQException
	 *             length of bytes is not enough to init cidr or inetType is
	 *             invalid
	 */
	public HAWQCidr(InetType inetType, byte[] bytes, int offset, short mask)
			throws HAWQException
	{
		super(inetType, bytes, offset, mask);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQCidr)
		{
			HAWQInet other = (HAWQInet) obj;
			return super.equals(other);
		}
		return false;
	}

	@Override
	public String toString()
	{
		String temp = super.toString();
		if (temp.indexOf('/') == -1)
		{
			if (getInetType() == InetType.IPV4)
				return temp + "/32";
			else
				return temp + "/128";
		}
		return temp;
	}
}
