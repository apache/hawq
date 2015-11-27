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


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.pivotal.hawq.mapreduce.HAWQException;

public class HAWQVarbitTest
{
	@Test
	public void testVarbitCtorWithCorrectString()
	{
		HAWQException exception = null;
		try
		{
			String varbitStr = "1010000100110010010010001001001011111101";
			HAWQVarbit varbit = new HAWQVarbit(varbitStr);
			assertTrue(varbit.toString().equals(varbitStr));
		}
		catch (HAWQException e)
		{
			e.printStackTrace();
			exception = e;
		}
		assertNull(exception);
	}

	@Test
	public void testVarbitCtorWithWrongString()
	{
		HAWQException exception = null;
		try
		{
			String pointStr = "1010000200110010010010001001001011111101";
			new HAWQVarbit(pointStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
		assertTrue(exception.getMessage().equals(
				"'2' is not a valid binary digit"));
	}

	@Test
	public void testVarbitCtorWithBytes()
	{
		byte[] bytes =
		{ (byte) 0xFF, (byte) 0xEF, 0x11, (byte) 0xC4, 0x58 };
		HAWQVarbit varbit = new HAWQVarbit(bytes, 37);
		assertTrue(varbit.toString().equals("1111111111101111000100011100010001011"));
	}

}
