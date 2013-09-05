package com.pivotal.hawq.mapreduce.datatype;

import static org.junit.Assert.*;

import java.sql.Date;
import java.sql.Timestamp;

import org.junit.Test;

public class HAWQTimestampTest
{

	@Test
	public void testAfterHAWQTimestamp()
	{
		long millisecond = 1000000;
		HAWQTimestamp date1 = new HAWQTimestamp(millisecond, true);
		HAWQTimestamp date2 = new HAWQTimestamp(millisecond, false);
		HAWQTimestamp date3 = new HAWQTimestamp(millisecond + 1, true);
		HAWQTimestamp date4 = new HAWQTimestamp(millisecond + 1, false);
		assertFalse(date1.after(date2));
		assertTrue(date2.after(date1));
		assertTrue(date1.after(date3));
		assertTrue(date4.after(date2));
	}

	@Test
	public void testAfterTimestamp()
	{
		long millisecond = 1000000;
		HAWQTimestamp date1 = new HAWQTimestamp(millisecond, true);
		HAWQTimestamp date2 = new HAWQTimestamp(millisecond, false);
		Timestamp date3 = new Timestamp(millisecond - 1);
		assertFalse(date1.after(date3));
		assertTrue(date2.after(date3));
	}

	@Test
	public void testAfterDate()
	{
		long millisecond = 1000000;
		HAWQTimestamp date1 = new HAWQTimestamp(millisecond, true);
		HAWQTimestamp date2 = new HAWQTimestamp(millisecond, false);
		Date date3 = new Date(millisecond - 1);
		assertFalse(date1.after(date3));
		assertTrue(date2.after(date3));
	}

	@Test
	public void testBeforeHAWQTimestamp()
	{
		long millisecond = 1000000;
		HAWQTimestamp date1 = new HAWQTimestamp(millisecond, true);
		HAWQTimestamp date2 = new HAWQTimestamp(millisecond, false);
		HAWQTimestamp date3 = new HAWQTimestamp(millisecond + 1, true);
		HAWQTimestamp date4 = new HAWQTimestamp(millisecond + 1, false);
		assertTrue(date1.before(date2));
		assertFalse(date2.before(date1));
		assertFalse(date1.before(date3));
		assertFalse(date4.before(date2));
	}

	@Test
	public void testBeforeTimestamp()
	{
		long millisecond = 1000000;
		HAWQTimestamp date1 = new HAWQTimestamp(millisecond, true);
		HAWQTimestamp date2 = new HAWQTimestamp(millisecond, false);
		Timestamp date3 = new Timestamp(millisecond - 1);
		assertTrue(date1.before(date3));
		assertFalse(date2.before(date3));
	}

	@Test
	public void testBeforeDate()
	{
		long millisecond = 1000000;
		HAWQTimestamp date1 = new HAWQTimestamp(millisecond, true);
		HAWQTimestamp date2 = new HAWQTimestamp(millisecond, false);
		Date date3 = new Date(millisecond - 1);
		assertTrue(date1.before(date3));
		assertFalse(date2.before(date3));
	}

}
