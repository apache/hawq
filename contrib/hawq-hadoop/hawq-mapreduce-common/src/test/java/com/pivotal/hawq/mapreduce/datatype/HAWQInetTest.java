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


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.pivotal.hawq.mapreduce.HAWQException;

public class HAWQInetTest
{
	@Test
	public void testInetCtorWithCorrectIPV4Str()
	{
		HAWQException exception = null;
		HAWQInet inet1 = null, inet2 = null;
		String ipv4Str1 = "192.168.1.255/24", ipv4Str2 = "192.168.1.255/32";
		try
		{
			inet1 = new HAWQInet(ipv4Str1);
			inet2 = new HAWQInet(ipv4Str2);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNull(exception);
		assertTrue(inet1.toString().equals(ipv4Str1));
		assertTrue(inet2.toString().equals(
				ipv4Str2.substring(0, ipv4Str2.indexOf('/'))));
	}

	@Test
	public void testInetCtorWithCorrectIPV6Str()
	{
		HAWQException exception = null;
		HAWQInet inet1 = null, inet2 = null, inet3 = null;
		String ipv6Str1 = "2001:db8:85a3:8d3:1319:8a2e:370:7344/64", ipv6Str2 = "2001:ef3::7344", ipv6Str3 = "::ec43/128";
		try
		{
			inet1 = new HAWQInet(ipv6Str1);
			inet2 = new HAWQInet(ipv6Str2);
			inet3 = new HAWQInet(ipv6Str3);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNull(exception);
		assertTrue(inet1.toString().equals(ipv6Str1));
		assertTrue(inet2.toString().equals(ipv6Str2));
		assertTrue(inet3.toString().equals(
				ipv6Str3.substring(0, ipv6Str3.indexOf('/'))));
	}

	@Test
	public void testInetCtorWithWrongIPV4Str1()
	{
		HAWQException exception = null;
		try
		{
			String str = "192.168.1.255/";
			new HAWQInet(str);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testInetCtorWithWrongIPV4Str2()
	{
		HAWQException exception = null;
		try
		{
			String str = "192.168.1/1";
			new HAWQInet(str);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testInetCtorWithWrongIPV6Str1()
	{
		HAWQException exception = null;
		try
		{
			String str = "2001:db8:85a3:8d3:1319:8a2e:370:7344/";
			new HAWQInet(str);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testInetCtorWithWrongIPV6Str2()
	{
		HAWQException exception = null;
		try
		{
			String str = "2001:db8:85a3:8d3:1319:8a2e:370:";
			new HAWQInet(str);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}
}
