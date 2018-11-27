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
 * Store value of point in database
 */
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
			value = value.replaceAll(" ", "");
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
		/*
		 * GPSQL-936
		 *
		 * Remove trailing ".0" for float/double to match DB output
		 */
		buffer.append('(')
			  .append(((Double) x).toString().replaceAll("\\.0$", ""))
			  .append(',')
			  .append(((Double) y).toString().replaceAll("\\.0$", ""))
			  .append(')');

		return buffer.toString();
	}
}
