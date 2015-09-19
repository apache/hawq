package com.pivotal.hawq.mapreduce.datatype;

import com.pivotal.hawq.mapreduce.HAWQException;

/**
 * Store value of circle in database
 */
public class HAWQCircle
{
	private HAWQPoint center;
	private double radius;

	/**
	 * Initialize a circle from string
	 * 
	 * @param value
	 *            the value that circle init from. Should be like this:
	 *            <(1.2,1.3),2.2>
	 * @throws HAWQException
	 *             when this value is not correct for circle
	 */
	public HAWQCircle(String value) throws HAWQException
	{
		if (value.startsWith("<") && value.endsWith(">"))
		{
			String[] pointStrs = value.substring(1, value.length() - 1).split(
					",");

			if (pointStrs.length != 3)
				throw new HAWQException("Cannot convert " + value
						+ " to HAWQCircle");

			String pointStr = pointStrs[0] + "," + pointStrs[1];

			try
			{
				init(new HAWQPoint(pointStr), Double.parseDouble(pointStrs[2]));
			}
			catch (Exception e)
			{
				throw new HAWQException("Cannot convert " + value
						+ " to HAWQCircle");
			}
		}
		else
		{
			throw new HAWQException("Cannot convert " + value
					+ " to HAWQCircle");
		}
	}

	/**
	 * Initialize a box by coordinates
	 * 
	 * @param centerX
	 *            abscissa of center
	 * @param centerY
	 *            ordinate of center
	 * @param radius
	 *            radius of this circle
	 */
	public HAWQCircle(double centerX, double centerY, double radius)
	{
		init(new HAWQPoint(centerX, centerY), radius);
	}

	/**
	 * Initialize a box by center point and radius
	 * 
	 * @param center
	 *            center point of this circle
	 * @param radius
	 *            radius of this circle
	 */
	public HAWQCircle(HAWQPoint center, double radius)
	{
		init(center, radius);
	}

	private void init(HAWQPoint center, double radius)
	{
		this.center = center;
		this.radius = radius;
	}

	/**
	 * Get center of this circle
	 * 
	 * @return circle center
	 */
	public HAWQPoint getCenter()
	{
		return center;
	}

	/**
	 * Get radius of this circle
	 * 
	 * @return circle radius
	 */
	public double getRadius()
	{
		return radius;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQCircle)
		{
			HAWQCircle other = (HAWQCircle) obj;
			return radius == other.getRadius()
					&& center.equals(other.getCenter());
		}
		return false;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append('<').append(center).append(',').append(radius)
				.append('>');
		/*
		 * GPSQL-936
		 * 
		 * Remove useless ".0" for float/double
		 */
		return buffer.toString().replace(".0", "");
	}
}
