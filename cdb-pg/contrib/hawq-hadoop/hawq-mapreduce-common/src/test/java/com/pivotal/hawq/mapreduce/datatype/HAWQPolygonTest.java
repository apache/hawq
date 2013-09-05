package com.pivotal.hawq.mapreduce.datatype;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.pivotal.hawq.mapreduce.HAWQException;

public class HAWQPolygonTest
{
	@Test
	public void testPolygonCtorWithCorrectStr()
	{
		String polygonStr = "((1,1),(5,0),(4,5))";
		HAWQException exception = null;
		HAWQPolygon polygon = null;
		try
		{
			polygon = new HAWQPolygon(polygonStr);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNull(exception);
		assertTrue(polygon.toString().equals(polygonStr));
		assertTrue(polygon.getBoundbox().equals(new HAWQBox(5, 5, 1, 0)));
	}
}
