package com.pivotal.hawq.mapreduce.datatype;

import static org.junit.Assert.*;

import java.sql.Date;

import org.junit.Test;

public class HAWQDateTest
{

	@Test
	public void testAfterHAWQDate()
	{
		long millisecond = 1000000;
		HAWQDate date1 = new HAWQDate(millisecond, true);
		HAWQDate date2 = new HAWQDate(millisecond, false);
		HAWQDate date3 = new HAWQDate(millisecond + 1, true);
		HAWQDate date4 = new HAWQDate(millisecond + 1, false);
		assertFalse(date1.after(date2));
		assertTrue(date2.after(date1));
		assertTrue(date1.after(date3));
		assertTrue(date4.after(date2));
	}

	@Test
	public void testAfterDate()
	{
		long millisecond = 1000000;
		HAWQDate date1 = new HAWQDate(millisecond, true);
		HAWQDate date2 = new HAWQDate(millisecond, false);
		Date date3 = new Date(millisecond - 1);
		assertFalse(date1.after(date3));
		assertTrue(date2.after(date3));
	}

	@Test
	public void testBeforeHAWQDate()
	{
		long millisecond = 1000000;
		HAWQDate date1 = new HAWQDate(millisecond, true);
		HAWQDate date2 = new HAWQDate(millisecond, false);
		HAWQDate date3 = new HAWQDate(millisecond + 1, true);
		HAWQDate date4 = new HAWQDate(millisecond + 1, false);
		assertTrue(date1.before(date2));
		assertFalse(date2.before(date1));
		assertFalse(date1.before(date3));
		assertFalse(date4.before(date2));
	}

	@Test
	public void testABeforeDate()
	{
		long millisecond = 1000000;
		HAWQDate date1 = new HAWQDate(millisecond, true);
		HAWQDate date2 = new HAWQDate(millisecond, false);
		Date date3 = new Date(millisecond - 1);
		assertTrue(date1.before(date3));
		assertFalse(date2.before(date3));
	}

	@Test
	public void testEqualsObject()
	{
		long millisecond = 1000000;
		Date date = new Date(millisecond);
		HAWQDate hawqDate = new HAWQDate(millisecond);
		assertTrue(hawqDate.equals(date));
		hawqDate = new HAWQDate(millisecond + 1);
		assertFalse(hawqDate.equals(date));
		hawqDate = new HAWQDate(millisecond, true);
		assertFalse(hawqDate.equals(date));
	}

}
