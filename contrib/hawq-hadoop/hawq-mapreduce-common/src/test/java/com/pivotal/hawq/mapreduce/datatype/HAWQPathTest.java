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

public class HAWQPathTest
{
	@Test
	public void testPathCtorWithCorrectString()
	{
		HAWQException exception = null;
		try
		{
			String pathStr1 = "((1.2,1.3),(2.2,2.3),(3.2,3.3))";
			HAWQPath path1 = new HAWQPath(pathStr1);
			assertTrue(path1.toString().equals(pathStr1));
			assertFalse(path1.isOpen());
			assertTrue(path1.getPoints().size() == 3);
			assertTrue(path1.getPoints().get(0).equals(new HAWQPoint(1.2, 1.3)));
			assertTrue(path1.getPoints().get(1).equals(new HAWQPoint(2.2, 2.3)));
			assertTrue(path1.getPoints().get(2).equals(new HAWQPoint(3.2, 3.3)));

			String pathStr2 = "[(1.2,1.3),(2.2,2.3),(3.2,3.3),(4.2,4.3)]";
			HAWQPath path2 = new HAWQPath(pathStr2);
			assertTrue(path2.toString().equals(pathStr2));
			assertTrue(path2.isOpen());
			assertTrue(path2.getPoints().size() == 4);
			assertTrue(path2.getPoints().get(0).equals(new HAWQPoint(1.2, 1.3)));
			assertTrue(path2.getPoints().get(1).equals(new HAWQPoint(2.2, 2.3)));
			assertTrue(path2.getPoints().get(2).equals(new HAWQPoint(3.2, 3.3)));
			assertTrue(path2.getPoints().get(3).equals(new HAWQPoint(4.2, 4.3)));
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNull(exception);
	}

	@Test
	public void testPathCtorWithWrongString1()
	{
		HAWQException exception = null;
		try
		{
			String pointStr = "((1.2,1.3),(2.2,2.3),(3.2,3.3)";
			new HAWQPath(pointStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testPathCtorWithWrongString2()
	{
		HAWQException exception = null;
		try
		{
			String pointStr = "1.2,1.3),(2.2,2.3),(3.2,3.3))";
			new HAWQPath(pointStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testPathCtorWithSomePoints()
	{
		HAWQPoint point1 = new HAWQPoint(1.2, 1.3);
		HAWQPoint point2 = new HAWQPoint(2.2, 2.3);
		HAWQPoint point3 = new HAWQPoint(3.2, 3.3);
		HAWQPath path1 = new HAWQPath(false, point1, point2, point3);
		assertTrue(path1.toString().equals("((1.2,1.3),(2.2,2.3),(3.2,3.3))"));
		assertFalse(path1.isOpen());
		assertTrue(path1.getPoints().get(0).equals(point1));
		assertTrue(path1.getPoints().get(1).equals(point2));
		assertTrue(path1.getPoints().get(2).equals(point3));

		HAWQPath path2 = new HAWQPath(true, point1, point2, point3);
		assertTrue(path2.toString().equals("[(1.2,1.3),(2.2,2.3),(3.2,3.3)]"));
		assertTrue(path2.isOpen());
		assertTrue(path2.getPoints().get(0).equals(point1));
		assertTrue(path2.getPoints().get(1).equals(point2));
		assertTrue(path2.getPoints().get(2).equals(point3));
	}

	@Test
	public void testPathequals()
	{
		HAWQPoint point1 = new HAWQPoint(1.2, 1.3);
		HAWQPoint point2 = new HAWQPoint(2.2, 2.3);
		HAWQPoint point3 = new HAWQPoint(3.2, 3.3);
		HAWQPoint point4 = new HAWQPoint(4.2, 4.3);
		HAWQPath path1 = new HAWQPath(false, point1, point2, point3);
		HAWQPath path2 = new HAWQPath(true, point1, point2, point3);
		HAWQPath path3 = new HAWQPath(false, point1, point2);
		HAWQPath path4 = new HAWQPath(false, point1, point2, point4);
		HAWQPath path5 = new HAWQPath(false, point1, point2, point3);
		assertFalse(path1.equals(path2));
		assertFalse(path1.equals(path3));
		assertFalse(path1.equals(path4));
		assertTrue(path1.equals(path5));
	}
}
