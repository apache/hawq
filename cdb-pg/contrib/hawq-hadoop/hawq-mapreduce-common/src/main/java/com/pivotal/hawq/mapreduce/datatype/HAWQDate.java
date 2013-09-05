package com.pivotal.hawq.mapreduce.datatype;

import java.sql.Date;

/*
 * GPSQL-937
 * 
 * Add HAWQDate and override toString() of java.sql.Date
 */
public class HAWQDate extends Date
{
	private static final long serialVersionUID = 1L;
	private boolean isBC;

	/**
	 * Deprecated. instead use the constructor Date(long date)
	 * 
	 * Constructs a Date object initialized with the given year, month, and day.
	 * 
	 * The result is undefined if a given argument is out of bounds.
	 * 
	 * @param year
	 *            the year minus 1900; must be 0 to 8099. (Note that 8099 is
	 *            9999 minus 1900.)
	 * @param month
	 *            0 to 11
	 * @param day
	 *            1 to 31
	 */
	@Deprecated
	public HAWQDate(int year, int month, int day)
	{
		this(year, month, day, false);
	}

	/**
	 * Deprecated. instead use the constructor Date(long date)
	 * 
	 * Constructs a Date object initialized with the given year, month, and day.
	 * 
	 * The result is undefined if a given argument is out of bounds.
	 * 
	 * @param year
	 *            the year minus 1900; must be 0 to 8099. (Note that 8099 is
	 *            9999 minus 1900.)
	 * @param month
	 *            0 to 11
	 * @param day
	 *            1 to 31
	 * @param isBC
	 *            whether this date is before christ
	 */
	@Deprecated
	public HAWQDate(int year, int month, int day, boolean isBC)
	{
		super(year, month, day);
		this.isBC = isBC;
	}

	/**
	 * Constructs a Date object using the given milliseconds time value. If the
	 * given milliseconds value contains time information, the driver will set
	 * the time components to the time in the default time zone (the time zone
	 * of the Java virtual machine running the application) that corresponds to
	 * zero GMT.
	 * 
	 * @param milliseconds
	 *            milliseconds since January 1, 1970, 00:00:00 GMT not to exceed
	 *            the milliseconds representation for the year 8099. A negative
	 *            number indicates the number of milliseconds before January 1,
	 *            1970, 00:00:00 GMT.
	 */
	public HAWQDate(long milliseconds)
	{
		this(milliseconds, false);
	}

	/**
	 * /** Constructs a Date object using the given milliseconds time value. If
	 * the given milliseconds value contains time information, the driver will
	 * set the time components to the time in the default time zone (the time
	 * zone of the Java virtual machine running the application) that
	 * corresponds to zero GMT.
	 * 
	 * @param milliseconds
	 *            milliseconds since January 1, 1970, 00:00:00 GMT not to exceed
	 *            the milliseconds representation for the year 8099. A negative
	 *            number indicates the number of milliseconds before January 1,
	 *            1970, 00:00:00 GMT.
	 * @param isBC
	 *            whether this is a date before christ
	 */
	public HAWQDate(long milliseconds, boolean isBC)
	{
		super(milliseconds);
		this.isBC = isBC;
	}

	/**
	 * Calendar time to Julian date conversions. Julian date is commonly used in
	 * astronomical applications, since it is numerically accurate and
	 * computationally simple. The algorithms here will accurately convert
	 * between Julian day and calendar date for all non-negative Julian days
	 * (i.e. from Nov 24, -4713 on).
	 * 
	 * These routines will be used by other date/time packages - thomas 97/02/25
	 * 
	 * Rewritten to eliminate overflow problems. This now allows the routines to
	 * work correctly for all Julian day counts from 0 to 2147483647 (Nov 24,
	 * -4713 to Jun 3, 5874898) assuming a 32-bit integer. Longer types should
	 * also work to the limits of their precision.
	 * 
	 * @param year
	 * @param month
	 * @param date
	 * @return Julian date
	 */
	public static int date2j(int year, int month, int date)
	{
		int julian;
		int century;

		if (month > 2)
		{
			month += 1;
			year += 4800;
		}
		else
		{
			month += 13;
			year += 4799;
		}

		century = year / 100;
		julian = year * 365 - 32167;
		julian += year / 4 - century + century / 4;
		julian += 7834 * month / 256 + date;

		return julian;
	}

	/**
	 * Get whether this is a date before christ
	 * 
	 * @return if this is a date before christ, return true, else return false
	 */
	public boolean isBC()
	{
		return isBC;
	}

	public boolean after(HAWQDate date)
	{
		if (isBC && !date.isBC())
			return false;
		else if (!isBC && date.isBC())
			return true;
		else if (isBC && date.isBC())
			return !super.after(date);
		else
			return super.after(date);
	}

	public boolean before(HAWQDate date)
	{
		if (isBC && !date.isBC())
			return true;
		else if (!isBC && date.isBC())
			return false;
		else if (isBC && date.isBC())
			return !super.before(date);
		else
			return super.before(date);
	}

	@Override
	public boolean after(java.util.Date date)
	{
		if (date instanceof HAWQDate)
			return after((HAWQDate) date);
		if (isBC)
			return false;
		return super.after(date);
	}

	@Override
	public boolean before(java.util.Date date)
	{
		if (date instanceof HAWQDate)
			return before((HAWQDate) date);
		if (isBC)
			return true;
		return super.before(date);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQDate)
			return super.equals((Date) obj)
					&& this.isBC == ((HAWQDate) obj).isBC();
		if (obj instanceof Date && !isBC)
			return super.equals((Date) obj);
		return false;
	}

	@Override
	public String toString()
	{
		return super.toString() + (isBC ? " BC" : "");
	}
}
