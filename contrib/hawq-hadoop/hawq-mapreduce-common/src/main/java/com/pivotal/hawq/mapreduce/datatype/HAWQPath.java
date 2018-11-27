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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.pivotal.hawq.mapreduce.HAWQException;

/**
 * Store value of path in database
 */
public class HAWQPath
{
	private List<HAWQPoint> points;
	private boolean open;

	/**
	 * Initialize a path from string
	 * 
	 * @param value
	 *            the value that path init from. Should be like this:
	 *            [(1.2,1.3),(2.2,2.3)] or ((3.3,3.4),(4.3,4.4),(5.4,5.5)).
	 *            [...] means this path is open and (...) means this path is
	 *            closed
	 * @throws HAWQException
	 *             when this string is not correct for path
	 */
	public HAWQPath(String value) throws HAWQException
	{
		boolean open;
		value = value.replaceAll(" ", "");
		if (value.startsWith("[") && value.endsWith("]"))
		{
			open = true;
		}
		else if (value.startsWith("(") && value.endsWith(")"))
		{
			open = false;
		}
		else
		{
			throw new HAWQException("Cannot convert " + value + " to HAWQPath");
		}
		String[] pointStrs = value.substring(1, value.length() - 1).split(",");
		if (pointStrs.length % 2 != 0)
			throw new HAWQException("Cannot convert " + value + " to HAWQPath");

		ArrayList<HAWQPoint> points = new ArrayList<HAWQPoint>();
		for (int i = 0; i < pointStrs.length; i += 2)
		{
			String pointStr = pointStrs[i] + "," + pointStrs[i + 1];
			try
			{
				points.add(new HAWQPoint(pointStr));
			}
			catch (HAWQException e)
			{
				throw new HAWQException("Cannot convert " + value
						+ " to HAWQPath");
			}
		}
		init(open, points);
	}

	/**
	 * Initialize a path from points
	 * 
	 * @param open
	 *            whether this path is open
	 * @param points
	 *            vertexes of this path
	 */
	public HAWQPath(boolean open, List<HAWQPoint> points)
	{
		init(open, points);
	}

	/**
	 * Initialize a path from points
	 * 
	 * @param open
	 *            whether this path is open
	 * @param points
	 *            vertexes of this path
	 */
	public HAWQPath(boolean open, HAWQPoint... points)
	{
		init(open, Arrays.asList(points));
	}

	private void init(boolean open, List<HAWQPoint> points)
	{
		this.points = points;
		this.open = open;
	}

	/**
	 * Get vertexes of this path
	 * 
	 * @return list of vertexes
	 */
	public List<HAWQPoint> getPoints()
	{
		return points;
	}

	/**
	 * Whether this path is open
	 * 
	 * @return true for open path and false of closed path
	 */
	public boolean isOpen()
	{
		return open;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQPath)
		{
			HAWQPath other = (HAWQPath) obj;
			if (open != other.open)
				return false;

			if (points.size() != other.getPoints().size())
				return false;

			for (int i = 0; i < points.size(); i++)
			{
				if (!points.get(i).equals(other.getPoints().get(i)))
					return false;
			}
			return true;
		}
		return false;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		if (open)
			buffer.append('[');
		else
			buffer.append('(');
		int numOfPoints = points.size();
		for (int i = 0; i < numOfPoints; ++i)
		{
			buffer.append(points.get(i));
			if (i != numOfPoints - 1)
				buffer.append(',');
		}
		if (open)
			buffer.append(']');
		else
			buffer.append(')');
		return buffer.toString();
	}
}
