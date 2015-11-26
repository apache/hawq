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

public class HAWQBoxTest
{
	@Test
	public void testBoxCtorWithCorrectString()
	{
		HAWQException exception = null;
		try
		{
			String boxStr = "(1.2,1.3),(1.3,1.4)";
			HAWQBox box = new HAWQBox(boxStr);
			assertTrue(box.toString().equals(boxStr));
			assertTrue(box.getPoint1().equals(new HAWQPoint(1.2, 1.3)));
			assertTrue(box.getPoint2().equals(new HAWQPoint(1.3, 1.4)));
		}
		catch (HAWQException e)
		{
			e.printStackTrace();
			exception = e;
		}
		assertNull(exception);
	}

	@Test
	public void testBoxCtorWithWrongString1()
	{
		HAWQException exception = null;
		try
		{
			String pointStr = "[(1.2,1.3),(1.3,1.4)";
			new HAWQBox(pointStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testBoxCtorWithWrongString2()
	{
		HAWQException exception = null;
		try
		{
			String pointStr = "[(1.2,1.3),(1.3,1.4]";
			new HAWQBox(pointStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testBoxCtorWithTwoPoints()
	{
		HAWQPoint point1 = new HAWQPoint(1.2, 1.3);
		HAWQPoint point2 = new HAWQPoint(2.2, 2.3);
		HAWQBox box = new HAWQBox(point1, point2);
		assertTrue(box.toString().equals("(1.2,1.3),(2.2,2.3)"));
	}

	@Test
	public void testBoxCtorWithFourDigits()
	{
		HAWQBox box = new HAWQBox(1.2, 1.3, 2.2, 2.3);
		assertTrue(box.toString().equals("(1.2,1.3),(2.2,2.3)"));
	}

	@Test
	public void testBoxEqual()
	{
		HAWQBox box1 = new HAWQBox(1.2, 1.3, 2.2, 2.3);
		HAWQBox box2 = new HAWQBox(1.2, 2.3, 2.2, 2.3);
		HAWQBox box3 = new HAWQBox(1.2, 1.3, 3.2, 2.3);
		HAWQBox box4 = new HAWQBox(1.2, 1.3, 2.2, 2.3);
		assertFalse(box1.equals(box2));
		assertFalse(box1.equals(box3));
		assertTrue(box1.equals(box4));
	}
}
