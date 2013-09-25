package com.pivotal.hawq.mapreduce.datatype;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Store value of time/timetz in database
 */
public class HAWQTime extends Time
{
	/*
	 * GPSQL-937
	 * 
	 * Add HAWQTime and override toString() of java.sql.Time
	 */
	private static final long serialVersionUID = 1L;
	private static long baseMillisecondOfDatabase = -1;

	private int millisecond;
	private int microsecond;
	private long allMilliseconds;

	private boolean hasTimezone = false;
	private int zoneSecondOffset = 0;

	static
	{
		try
		{
			SimpleDateFormat dateFormat = new SimpleDateFormat(
					"HH:mm:ss.SSSSSS");
			baseMillisecondOfDatabase = dateFormat.parse("00:00:00.000000")
					.getTime();
		}
		catch (ParseException e)
		{}
	}

	/**
	 * Deprecated. Use the constructor that takes a milliseconds value in place
	 * of this constructor
	 * 
	 * Constructs a Time object initialized with the given values for the hour,
	 * minute, and second. The driver sets the date components to January 1,
	 * 1970. Any method that attempts to access the date components of a Time
	 * object will throw a java.lang.IllegalArgumentException.
	 * 
	 * The result is undefined if a given argument is out of bounds.
	 * 
	 * @param hour
	 *            0 to 23
	 * @param minute
	 *            0 to 59
	 * @param second
	 *            0 to 59
	 */
	@Deprecated
	public HAWQTime(int hour, int minute, int second)
	{
		this(hour, minute, second, 0, 0);
	}

	/**
	 * Deprecated. Use the constructor that takes a milliseconds value in place
	 * of this constructor
	 * 
	 * Constructs a Time object initialized with the given values for the hour,
	 * minute, and second. The driver sets the date components to January 1,
	 * 1970. Any method that attempts to access the date components of a Time
	 * object will throw a java.lang.IllegalArgumentException.
	 * 
	 * The result is undefined if a given argument is out of bounds.
	 * 
	 * @param hour
	 *            0 to 23
	 * @param minute
	 *            0 to 59
	 * @param second
	 *            0 to 59
	 * @param millisecond
	 *            0 to 999
	 * @param microsecond
	 *            0 to 999
	 */
	@Deprecated
	public HAWQTime(int hour, int minute, int second, int millisecond,
			int microsecond)
	{
		super(hour, minute, second);
		this.millisecond = millisecond;
		this.microsecond = microsecond;
	}

	/**
	 * Constructs a Time object using a milliseconds time value.
	 * 
	 * @param time
	 *            milliseconds since January 1, 1970, 00:00:00 GMT; a negative
	 *            number is milliseconds before January 1, 1970, 00:00:00 GMT
	 */
	public HAWQTime(long time)
	{
		super(time);
	}

	/**
	 * Constructs a Time object using a milliseconds time value.
	 * 
	 * @param time
	 *            milliseconds since January 1, 1970, 00:00:00 GMT; a negative
	 *            number is milliseconds before January 1, 1970, 00:00:00 GMT
	 * @param microsecond
	 *            microseconds in this time
	 */
	public HAWQTime(long time, int microsecond)
	{
		super(baseMillisecondOfDatabase + time);
		this.millisecond = (int) (time % 1000);
		this.microsecond = microsecond;
	}

	/**
	 * Constructs a Time object using a milliseconds time value.
	 * 
	 * @param time
	 *            milliseconds since January 1, 1970, 00:00:00 GMT; a negative
	 *            number is milliseconds before January 1, 1970, 00:00:00 GMT
	 * @param microsecond
	 *            microseconds in this time
	 * @param zoneSecondOffset
	 *            time zone offset in second
	 */
	public HAWQTime(long time, int microsecond, int zoneSecondOffset)
	{
		super(time + zoneSecondOffset * 1000);
		this.millisecond = (int) (time % 1000);
		this.microsecond = microsecond;
		this.zoneSecondOffset = zoneSecondOffset;
		this.hasTimezone = true;
		this.allMilliseconds = time;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		if (hasTimezone)
		{
			long time = allMilliseconds; // all milliseconds
			time /= 1000; // all seconds
			int second = (int) (time % 60);
			time /= 60; // all minutes
			int minute = (int) (time % 60);
			int hour = (int) (time /= 60);
			buffer.append(String.format("%02d:%02d:%02d", hour, minute, second));

			if (millisecond != 0 || microsecond != 0)
			{
				buffer.append(String.format(".%03d", millisecond));
				if (microsecond != 0)
					buffer.append(String.format("%03d", microsecond));

				while (true)
				{
					int temp = buffer.length();
					if (buffer.charAt(temp - 1) != '0')
						break;
					buffer.deleteCharAt(temp - 1);
				}

			}

			if (zoneSecondOffset == 0)
				buffer.append("+00");
			else
			{
				int allZoneMinutes = zoneSecondOffset / 60;
				int zoneMinute = allZoneMinutes % 60;
				int zoneHour = allZoneMinutes / 60;
				if (zoneHour > 0)
					buffer.append(String.format("-%02d", zoneHour));
				else
					buffer.append(String.format("+%02d", -zoneHour));
				if (zoneMinute != 0)
				{
					if (zoneMinute < 0)
						zoneMinute = -zoneMinute;
					buffer.append(":" + zoneMinute);
				}
			}

			return buffer.toString();
		}

		if (millisecond == 0 && microsecond == 0)
			return super.toString();

		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
		buffer.append(dateFormat.format(this));
		if (microsecond != 0)
			buffer.append(String.format("%03d", microsecond));

		while (true)
		{
			int temp = buffer.length();
			if (buffer.charAt(temp - 1) != '0')
				break;
			buffer.deleteCharAt(temp - 1);
		}
		return buffer.toString();
	}
}
