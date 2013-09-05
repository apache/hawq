package com.pivotal.hawq.mapreduce.datatype;

import org.junit.Test;

import com.pivotal.hawq.mapreduce.HAWQException;

import static org.junit.Assert.*;

public class HAWQPointTest
{
	@Test
	public void testPointCtorWithCorrectString()
	{
		HAWQException exception = null;
		try
		{
			String pointStr = "(1.2,1.3)";
			HAWQPoint point = new HAWQPoint(pointStr);
			assertTrue(point.toString().equals(pointStr));
			assertTrue(point.getX() == 1.2);
			assertTrue(point.getY() == 1.3);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNull(exception);
	}

	@Test
	public void testPointCtorWithWrongString()
	{
		HAWQException exception = null;
		try
		{
			String pointStr = "(1.2,1.3";
			new HAWQPoint(pointStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testPointCtorWithTwoDouble()
	{
		double x = 1.2, y = 1000;
		HAWQPoint point = new HAWQPoint(x, y);
		assertTrue(point.toString().equals("(1.2,1000)"));
	}
	
	@Test
	public void testPointEqual() {
		HAWQPoint point1 = new HAWQPoint(1.2, 1.3);
		HAWQPoint point2 = new HAWQPoint(1.2, 2.3);
		HAWQPoint point3 = new HAWQPoint(3.2, 1.3);
		HAWQPoint point4 = new HAWQPoint(1.2, 1.3);
		assertFalse(point1.equals(point2));
		assertFalse(point1.equals(point3));
		assertTrue(point1.equals(point4));
	}
}
