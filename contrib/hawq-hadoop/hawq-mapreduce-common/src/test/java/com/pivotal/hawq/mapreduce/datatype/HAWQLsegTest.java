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


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.pivotal.hawq.mapreduce.HAWQException;

public class HAWQLsegTest
{
	@Test
	public void testLsegCtorWithCorrectString()
	{
		Exception exception = null;
		try
		{
			String lsegStr = "[(1.2,1.3),(1.3,1.4)]";
			HAWQLseg lseg = new HAWQLseg(lsegStr);
			assertTrue(lseg.toString().equals(lsegStr));
			assertTrue(lseg.getPoint1().equals(new HAWQPoint(1.2, 1.3)));
			assertTrue(lseg.getPoint2().equals(new HAWQPoint(1.3, 1.4)));
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNull(exception);
	}

	@Test
	public void testLsegCtorWithWrongString1()
	{
		Exception exception = null;
		try
		{
			String pointStr = "[(1.2,1.3),(1.3,1.4)";
			new HAWQLseg(pointStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testLsegCtorWithWrongString2()
	{
		Exception exception = null;
		try
		{
			String pointStr = "[(1.2,1.3),(1.3,1.4]";
			new HAWQLseg(pointStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testLsegCtorWithTwoPoints()
	{
		HAWQPoint point1 = new HAWQPoint(1.2, 1.3);
		HAWQPoint point2 = new HAWQPoint(2.2, 2.3);
		HAWQLseg lseg = new HAWQLseg(point1, point2);
		assertTrue(lseg.toString().equals("[(1.2,1.3),(2.2,2.3)]"));
	}

	@Test
	public void testLsegCtorWithFourDigits()
	{
		HAWQLseg lseg = new HAWQLseg(1.2, 1.3, 2.2, 2.3);
		assertTrue(lseg.toString().equals("[(1.2,1.3),(2.2,2.3)]"));
	}

	@Test
	public void testLsegequals()
	{
		HAWQLseg lseg1 = new HAWQLseg(1.2, 1.3, 2.2, 2.3);
		HAWQLseg lseg2 = new HAWQLseg(1.2, 2.3, 2.2, 2.3);
		HAWQLseg lseg3 = new HAWQLseg(1.2, 1.3, 3.2, 2.3);
		HAWQLseg lseg4 = new HAWQLseg(1.2, 1.3, 2.2, 2.3);
		assertFalse(lseg1.equals(lseg2));
		assertFalse(lseg1.equals(lseg3));
		assertTrue(lseg1.equals(lseg4));
	}
}
