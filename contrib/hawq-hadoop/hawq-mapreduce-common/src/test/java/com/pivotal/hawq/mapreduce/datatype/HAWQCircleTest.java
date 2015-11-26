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

public class HAWQCircleTest
{
	@Test
	public void testCircleCtorWithCorrectString()
	{
		HAWQException exception = null;
		try
		{
			String circleStr = "<(1.2,1.3),10>";
			HAWQCircle circle = new HAWQCircle(circleStr);
			assertTrue(circle.toString().equals(circleStr));
			assertTrue(circle.getCenter().equals(new HAWQPoint(1.2, 1.3)));
			assertTrue(circle.getRadius() == 10f);
		}
		catch (HAWQException e)
		{
			e.printStackTrace();
			exception = e;
		}
		assertNull(exception);
	}

	@Test
	public void testCircleCtorWithWrongString1()
	{
		HAWQException exception = null;
		try
		{
			String pointStr = "<(1.2,1.3),3.4";
			new HAWQCircle(pointStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testCircleCtorWithWrongString2()
	{
		HAWQException exception = null;
		try
		{
			String pointStr = "<1.2,1.3,3.4>";
			new HAWQCircle(pointStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testCircleCtorWithCenterAndRadius()
	{
		HAWQPoint center = new HAWQPoint(20, 1.3);
		HAWQCircle circle = new HAWQCircle(center, 2.3);
		assertTrue(circle.toString().equals("<(20,1.3),2.3>"));
	}

	@Test
	public void testCircleCtorWithFourDigits()
	{
		HAWQCircle circle = new HAWQCircle(1.2, 1.3, 2.3);
		assertTrue(circle.toString().equals("<(1.2,1.3),2.3>"));
	}

	@Test
	public void testCircleequals()
	{
		HAWQCircle circle1 = new HAWQCircle(1.2, 1.3, 2.3);
		HAWQCircle circle2 = new HAWQCircle(1.2, 2.3, 2.3);
		HAWQCircle circle3 = new HAWQCircle(1.2, 1.3, 3.3);
		HAWQCircle circle4 = new HAWQCircle(1.2, 1.3, 2.3);
		assertFalse(circle1.equals(circle2));
		assertFalse(circle1.equals(circle3));
		assertTrue(circle1.equals(circle4));
	}
}
