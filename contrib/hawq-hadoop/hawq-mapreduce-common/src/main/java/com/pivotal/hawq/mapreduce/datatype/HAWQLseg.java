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
 * Store value of lseg in database
 */
public class HAWQLseg
{
	private HAWQPoint point1;
	private HAWQPoint point2;

	/**
	 * Initialize a line segment from string
	 * 
	 * @param value
	 *            the value that line segment init from. Should be like this:
	 *            [(1.2,1.3),(2.2,2.3)]
	 * @throws HAWQException
	 *             when this string is not correct for line segment
	 */
	public HAWQLseg(String value) throws HAWQException
	{
		if (value.startsWith("[") && value.endsWith("]"))
		{
			String[] pointStrs = value.substring(1, value.length() - 1).split(
					",");

			if (pointStrs.length != 4)
				throw new HAWQException("Cannot convert " + value
						+ " to HAWQLseg");

			String pointStr1 = pointStrs[0] + "," + pointStrs[1];
			String pointStr2 = pointStrs[2] + "," + pointStrs[3];

			try
			{
				init(new HAWQPoint(pointStr1), new HAWQPoint(pointStr2));
			}
			catch (HAWQException e)
			{
				throw new HAWQException("Cannot convert " + value
						+ " to HAWQLseg");
			}
		}
		else
		{
			throw new HAWQException("Cannot convert " + value + " to HAWQLseg");
		}
	}

	/**
	 * Initialize a line segment by coordinates
	 * 
	 * @param x1
	 *            abscissa of first endpoint
	 * @param y1
	 *            ordinate of first endpoint
	 * @param x2
	 *            abscissa of second endpoint
	 * @param y2
	 *            ordinate of second endpoint
	 */
	public HAWQLseg(double x1, double y1, double x2, double y2)
	{
		init(new HAWQPoint(x1, y1), new HAWQPoint(x2, y2));
	}

	/**
	 * Initialize a line segment by endpoints
	 * 
	 * @param point1
	 *            first endpoint
	 * @param point2
	 *            second endpoint
	 */
	public HAWQLseg(HAWQPoint point1, HAWQPoint point2)
	{
		init(point1, point2);
	}

	private void init(HAWQPoint point1, HAWQPoint point2)
	{
		this.point1 = point1;
		this.point2 = point2;
	}

	/**
	 * Get first endpoint
	 * 
	 * @return first endpoint
	 */
	public HAWQPoint getPoint1()
	{
		return point1;
	}

	/**
	 * Get second endpoint
	 * 
	 * @return second endpoint
	 */
	public HAWQPoint getPoint2()
	{
		return point2;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQLseg)
		{
			HAWQLseg other = (HAWQLseg) obj;
			return point1.equals(other.getPoint1())
					&& point2.equals(other.getPoint2());
		}
		return false;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append('[').append(point1.toString()).append(',')
				.append(point2.toString()).append(']');
		return buffer.toString();
	}
}
