package com.pivotal.hawq.mapreduce.datatype;

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
