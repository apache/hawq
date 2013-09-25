package com.pivotal.hawq.mapreduce.datatype;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * Store value of timestamp/timestamptz in database
 */
public class HAWQTimestamp extends Timestamp
{
	private static final long serialVersionUID = 1L;
	private boolean isBC;
	private int millisecond;
	private int microsecond;

	/**
	 * Deprecated. instead use the constructor HAWQTimestamp(long millis)
	 * 
	 * Constructs a HAWQTimestamp object initialized with the given values.
	 * 
	 * @param year
	 *            the year minus 1900
	 * @param month
	 *            0 to 11
	 * @param date
	 *            1 to 31
	 * @param hour
	 *            0 to 23
	 * @param minute
	 *            0 to 59
	 * @param second
	 *            0 to 59
	 * @param nano
	 *            0 to 999,999,999
	 */
	@Deprecated
	public HAWQTimestamp(int year, int month, int date, int hour, int minute,
			int second, int nano)
	{
		this(year, month, date, hour, minute, second, nano, false);
	}

	/**
	 * Deprecated. instead use the constructor HAWQTimestamp(long millis,
	 * boolean isBC)
	 * 
	 * Constructs a HAWQTimestamp object initialized with the given values.
	 * 
	 * @param year
	 *            the year minus 1900
	 * @param month
	 *            0 to 11
	 * @param date
	 *            1 to 31
	 * @param hour
	 *            0 to 23
	 * @param minute
	 *            0 to 59
	 * @param second
	 *            0 to 59
	 * @param nano
	 *            0 to 999,999,999
	 * @param isBC
	 *            whether this timestamp is before christ
	 */
	@Deprecated
	public HAWQTimestamp(int year, int month, int date, int hour, int minute,
			int second, int nano, boolean isBC)
	{
		super(year, month, date, hour, minute, second, nano);
		this.isBC = isBC;
		this.microsecond = (nano / 1000) % 1000;
		this.millisecond = nano / 1000000;
	}

	/**
	 * Allocates a Timestamp object and initializes it to represent the
	 * specified number of milliseconds since the standard base time known as
	 * "the epoch", namely January 1, 1970, 00:00:00 GMT.
	 * 
	 * @param milliseconds
	 *            the milliseconds since January 1, 1970, 00:00:00 GMT.
	 */
	public HAWQTimestamp(long milliseconds)
	{
		this(milliseconds, 0);
	}

	/**
	 * Allocates a Timestamp object and initializes it to represent the
	 * specified number of milliseconds since the standard base time known as
	 * "the epoch", namely January 1, 1970, 00:00:00 GMT.
	 * 
	 * @param milliseconds
	 *            the milliseconds since January 1, 1970, 00:00:00 GMT.
	 * @param microsecond
	 *            the microseconds in the timestamp
	 */
	public HAWQTimestamp(long milliseconds, int microsecond)
	{
		this(milliseconds, microsecond, false);
	}

	public HAWQTimestamp(long milliseconds, boolean isBC)
	{
		this(milliseconds, 0, isBC);
	}

	/**
	 * Allocates a Timestamp object and initializes it to represent the
	 * specified number of milliseconds since the standard base time known as
	 * "the epoch", namely January 1, 1970, 00:00:00 GMT.
	 * 
	 * @param milliseconds
	 *            the milliseconds since January 1, 1970, 00:00:00 GMT.
	 * @param microsecond
	 *            the microseconds in the timestamp
	 * @param isBC
	 *            whether this timestamp is before christ
	 */
	public HAWQTimestamp(long milliseconds, int microsecond, boolean isBC)
	{
		super(milliseconds);
		this.millisecond = (int) (milliseconds % 1000);
		this.microsecond = microsecond;
		this.isBC = isBC;
	}

	/**
	 * Initialize a Timestamp object by Date(represents a day) and microseconds
	 * in this day
	 * 
	 * @param date
	 *            day
	 * @param microsecondOutDay
	 *            micorseconds in this day
	 * @param isBC
	 *            whether this timestamp is before christ
	 */
	public HAWQTimestamp(Date date, long microsecondOutDay, boolean isBC)
	{
		this(date, microsecondOutDay, isBC, 0);
	}

	/**
	 * Initialize a Timestamp object by Date(represents a day) and microseconds
	 * in this day
	 * 
	 * @param date
	 *            day
	 * @param microsecondOutDay
	 *            micorseconds in this day
	 * @param isBC
	 *            whether this timestamp is before christ
	 * @param timezoneOffset
	 *            time zone offset
	 */
	public HAWQTimestamp(Date date, long microsecondOutDay, boolean isBC,
			long timezoneOffset)
	{
		super(date.getTime() + microsecondOutDay / 1000 + timezoneOffset);

		int microsecondOutSecond = (int) (microsecondOutDay % 1000000);
		if (microsecondOutSecond < 0)
			microsecondOutSecond += 1000000;
		this.millisecond = microsecondOutSecond / 1000;
		this.microsecond = microsecondOutSecond % 1000;
		this.isBC = isBC;
	}

	/**
	 * Get whether this timestamp is before christ
	 * 
	 * @return true is this timestamp is before christ, other false
	 */
	public boolean isBC()
	{
		return isBC;
	}

	public boolean after(HAWQTimestamp ts)
	{
		HAWQTimestamp timestamp = (HAWQTimestamp) ts;
		if (isBC && !timestamp.isBC)
			return false;
		else if (!isBC && timestamp.isBC)
			return true;
		else if (isBC && timestamp.isBC)
			return !super.after(ts);
		else
			return super.after(ts);
	}

	@Override
	public boolean after(Timestamp ts)
	{
		if (ts instanceof HAWQTimestamp)
			return after((HAWQTimestamp) ts);
		if (isBC)
			return false;
		return super.after(ts);
	}

	@Override
	public boolean after(java.util.Date when)
	{
		if (when instanceof HAWQTimestamp)
			return after((HAWQTimestamp) when);
		if (isBC)
			return false;
		return super.after(when);
	}

	public boolean before(HAWQTimestamp timestamp)
	{
		if (isBC && !timestamp.isBC())
			return true;
		else if (!isBC && timestamp.isBC())
			return false;
		else if (isBC && timestamp.isBC())
			return !super.before(timestamp);
		else
			return super.before(timestamp);
	}

	@Override
	public boolean before(Timestamp ts)
	{
		if (ts instanceof HAWQTimestamp)
			return before((HAWQTimestamp) ts);
		if (isBC)
			return true;
		return super.before(ts);
	}

	@Override
	public boolean before(java.util.Date when)
	{
		if (when instanceof HAWQTimestamp)
			return before((HAWQTimestamp) when);
		if (isBC)
			return true;
		return super.before(when);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQTimestamp)
			return super.equals((Timestamp) obj)
					&& this.isBC == ((HAWQTimestamp) obj).isBC();
		if (obj instanceof Timestamp && !isBC)
			return super.equals((Timestamp) obj);
		return false;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		buffer.append(dateFormat.format(this));
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

		if (isBC)
			buffer.append(" BC");
		return buffer.toString();
	}
}
