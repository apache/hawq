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

public class HAWQIntervalTest
{
	@Test
	public void testIntervalCtorWithCorrectInput()
	{
		HAWQException exception = null;
		try
		{
			HAWQInterval interval = new HAWQInterval(1000, 11, 4234, 2, 40, 17,
					200, 10);
			assertTrue(interval.toString().equals(
					"1000 years 11 mons 4234 days 02:40:17.20001"));
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNull(exception);
	}

	@Test
	public void testIntervalCtorWithWrongMonth()
	{
		HAWQException exception = null;
		try
		{
			new HAWQInterval(1000, 12, 4234, 2, 40, 17, 789, 123);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
		assertTrue(exception.toString().contains(
				"Months in interval should between 0 and 11"));
	}

	@Test
	public void testIntervalCtorWithWrongHour()
	{
		HAWQException exception = null;
		try
		{
			new HAWQInterval(1000, 10, 4234, 24, 40, 17, 789, 123);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
		assertTrue(exception.toString().contains(
				"Hours in interval should between 0 and 23"));
	}

	@Test
	public void testIntervalCtorWithWrongMinute()
	{
		HAWQException exception = null;
		try
		{
			new HAWQInterval(1000, 10, 4234, 4, -1, 17, 789, 123);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
		assertTrue(exception.toString().contains(
				"Minutes in interval should between 0 and 59"));
	}

	@Test
	public void testIntervalCtorWithWrongSecond()
	{
		HAWQException exception = null;
		try
		{
			new HAWQInterval(1000, 10, 4234, 4, 40, 60, 789, 123);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
		assertTrue(exception.toString().contains(
				"Seconds in interval should between 0 and 59"));
	}

	@Test
	public void testIntervalCtorWithWrongMillisecond()
	{
		HAWQException exception = null;
		try
		{
			new HAWQInterval(1000, 10, 4234, 4, 40, 59, 1000, 123);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
		assertTrue(exception.toString().contains(
				"Milliseconds in interval should between 0 and 999"));
	}

	@Test
	public void testIntervalCtorWithWrongMicrosecond()
	{
		HAWQException exception = null;
		try
		{
			new HAWQInterval(1000, 10, 4234, 4, 40, 59, 100, 1111);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
		assertTrue(exception.toString().contains(
				"Microseconds in interval should between 0 and 999"));
	}

	@Test
	public void testIntervalequals()
	{
		HAWQException exception = null;
		try
		{
			HAWQInterval interval1 = new HAWQInterval(1000, 11, 4234, 2, 40, 17);
			HAWQInterval interval2 = new HAWQInterval(1000, 10, 4234, 2, 40, 17);
			HAWQInterval interval3 = new HAWQInterval(1000, 11, 4233, 2, 40, 17);
			HAWQInterval interval4 = new HAWQInterval(1000, 11, 4234, 1, 40, 17);
			HAWQInterval interval5 = new HAWQInterval(1000, 11, 4234, 2, 41, 17);
			HAWQInterval interval6 = new HAWQInterval(1000, 11, 4234, 2, 40, 10);
			HAWQInterval interval7 = new HAWQInterval(1000, 11, 4234, 2, 40, 17);
			assertFalse(interval1.equals(interval2));
			assertFalse(interval1.equals(interval3));
			assertFalse(interval1.equals(interval4));
			assertFalse(interval1.equals(interval5));
			assertFalse(interval1.equals(interval6));
			assertTrue(interval1.equals(interval7));
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNull(exception);
	}

	@Test
	public void testIntervaltoString()
	{
		HAWQException exception = null;
		try
		{
			HAWQInterval interval = new HAWQInterval(1000, 0, 0, 0, 0, 0);
			assertTrue(interval.toString().equals("1000 years"));
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNull(exception);
	}
}
