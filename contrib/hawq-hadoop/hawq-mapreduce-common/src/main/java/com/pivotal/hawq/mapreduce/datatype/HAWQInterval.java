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


import com.pivotal.hawq.mapreduce.HAWQException;

/**
 * Store value of interval in database
 */
public class HAWQInterval
{
	private int year;
	private int month;
	private int day;
	private int hour;
	private int minute;
	private int second;
	private int millisecond;
	private int microsecond;

	/**
	 * Initialize an interval
	 * 
	 * @param year
	 *            years in this interval
	 * @param month
	 *            month in this interval, 0 to 11
	 * @param day
	 *            day in this interval
	 * @param hour
	 *            hour in this interval, 0 to 23
	 * @param minute
	 *            minute in this interval, 0 to 59
	 * @param second
	 *            second in this interval, 0 to 59
	 * @throws HAWQException
	 *             when parameter is invalid
	 */
	public HAWQInterval(int year, int month, int day, int hour, int minute,
			int second) throws HAWQException
	{
		this(year, month, day, hour, minute, second, 0, 0);
	}

	/**
	 * Initialize an interval
	 * 
	 * @param year
	 *            years in this interval
	 * @param month
	 *            month in this interval, 0 to 11
	 * @param day
	 *            day in this interval
	 * @param hour
	 *            hour in this interval, 0 to 23
	 * @param minute
	 *            minute in this interval, 0 to 59
	 * @param second
	 *            second in this interval, 0 to 59
	 * @param millisecond
	 *            millisecond in this interval, 0 to 999
	 * @param microsecond
	 *            microsecond in this interval, 0 to 999
	 * @throws HAWQException
	 *             when parameter is invalid
	 */
	public HAWQInterval(int year, int month, int day, int hour, int minute,
			int second, int millisecond, int microsecond) throws HAWQException
	{
		if (month < 0 || month > 11)
			throw new HAWQException(
					"Months in interval should between 0 and 11");
		if (hour < 0 || hour > 23)
			throw new HAWQException("Hours in interval should between 0 and 23");
		if (minute < 0 || minute > 59)
			throw new HAWQException(
					"Minutes in interval should between 0 and 59");
		if (second < 0 || second > 59)
			throw new HAWQException(
					"Seconds in interval should between 0 and 59");
		if (millisecond < 0 || millisecond > 999)
			throw new HAWQException(
					"Milliseconds in interval should between 0 and 999");
		if (microsecond < 0 || microsecond > 999)
			throw new HAWQException(
					"Microseconds in interval should between 0 and 999");

		this.year = year;
		this.month = month;
		this.day = day;
		this.hour = hour;
		this.minute = minute;
		this.second = second;
		this.millisecond = millisecond;
		this.microsecond = microsecond;
	}

	/**
	 * Get how many years in this interval
	 * 
	 * @return years amount of years
	 */
	public int getYears()
	{
		return year;
	}

	/**
	 * Get how many months in this interval(0-11)
	 * 
	 * @return months amount of months
	 */
	public int getMonths()
	{
		return month;
	}

	/**
	 * Get how many days in this interval
	 * 
	 * @return days amount of days
	 */
	public int getDays()
	{
		return day;
	}

	/**
	 * Get how many hours in this interval(0-23)
	 * 
	 * @return hours amount of hours
	 */
	public int getHours()
	{
		return hour;
	}

	/**
	 * Get how many minutes in this interval(0-59)
	 * 
	 * @return minutes amount of minutes
	 */
	public int getMinutes()
	{
		return minute;
	}

	/**
	 * Get how many seconds in this interval(0-59)
	 * 
	 * @return seconds amount of seconds
	 */
	public int getSeconds()
	{
		return second;
	}

	/**
	 * Get how many milliseconds in this interval(0-999)
	 * 
	 * @return milliseconds amount of milliseconds
	 */
	public int getMilliseconds()
	{
		return millisecond;
	}

	/**
	 * Get how many microseconds in this interval(0-999)
	 * 
	 * @return microseconds amount of microseconds
	 */
	public int getMicroseconds()
	{
		return microsecond;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQInterval)
		{
			HAWQInterval other = (HAWQInterval) obj;
			return year == other.getYears() && month == other.getMonths()
					&& day == other.getDays() && hour == other.getHours()
					&& minute == other.getMinutes()
					&& second == other.getSeconds()
					&& millisecond == other.getMilliseconds()
					&& microsecond == other.getMicroseconds();
		}
		return false;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		if (year != 0)
		{
			buffer.append(year);
			buffer.append(" year");
			if (year != 1)
				buffer.append('s');
			buffer.append(' ');
		}
		if (month != 0)
		{
			buffer.append(month);
			buffer.append(" mon");
			if (month != 1)
				buffer.append('s');
			buffer.append(' ');
		}
		if (day != 0)
		{
			buffer.append(day);
			buffer.append(" day");
			if (day != 1)
				buffer.append('s');
			buffer.append(' ');
		}
		if (hour != 0 || minute != 0 || second != 0 || microsecond != 0
				|| millisecond != 0)
		{
			buffer.append(String.format("%02d", hour)).append(':');
			buffer.append(String.format("%02d", minute)).append(':');
			buffer.append(String.format("%02d", second));

			if (millisecond != 0 || microsecond != 0)
			{
				buffer.append('.').append(String.format("%03d", millisecond));
				if (microsecond != 0)
					buffer.append(String.format("%03d", microsecond));

				while (true)
				{
					int temp = buffer.length();
					if (buffer.charAt(temp - 1) != '0'
							&& buffer.charAt(temp - 1) != ' ')
						break;
					buffer.deleteCharAt(temp - 1);
				}
			}
		}

		while (true)
		{
			int temp = buffer.length();
			if (buffer.charAt(temp - 1) != ' ')
				break;
			buffer.deleteCharAt(temp - 1);
		}
		return buffer.toString();
	}
}
