package com.pivotal.hawq.mapreduce.datatype;

import com.pivotal.hawq.mapreduce.HAWQException;

public class HAWQPoint
{
	private double x;
	private double y;

	/**
	 * Initialize a point from string
	 * 
	 * @param value
	 *            the value that point init from. Should be like this: (1.2,1.3)
	 * @throws HAWQException
	 *             when this string is not correct for point
	 */
	public HAWQPoint(String value) throws HAWQException
	{
		if (value.startsWith("(") && value.endsWith(")"))
		{
			value.replaceAll(" ", "");
			int posOfComma = value.indexOf(',');
			double x = Double.parseDouble(value.substring(1, posOfComma));
			double y = Double.parseDouble(value.substring(posOfComma + 1,
					value.indexOf(')')));
			init(x, y);
		}
		else
		{
			throw new HAWQException("Cannot convert " + value + " to HAWQPoint");
		}
	}

	/**
	 * Initialize a point by coordinates
	 * 
	 * @param x
	 *            abscissa of this point
	 * @param y
	 *            ardinate of this point
	 */
	public HAWQPoint(double x, double y)
	{
		init(x, y);
	}

	private void init(double x, double y)
	{
		this.x = x;
		this.y = y;
	}

	/**
	 * Get abscissa
	 * 
	 * @return abscissa of this point
	 */
	public double getX()
	{
		return x;
	}

	/**
	 * Get ordinate
	 * 
	 * @return ordinate of this point
	 */
	public double getY()
	{
		return y;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQPoint)
		{
			HAWQPoint other = (HAWQPoint) obj;
			return x == other.getX() && y == other.getY();
		}
		return false;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append('(').append(x).append(',').append(y).append(')');
		/*
		 * GPSQL-936
		 * 
		 * Remove useless ".0" for float/double
		 */
		return buffer.toString().replace(".0", "");
	}
}
