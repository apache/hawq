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
import java.util.List;

import com.pivotal.hawq.mapreduce.HAWQException;

/**
 * Store value of polygon in database
 */
public class HAWQPolygon
{
	private HAWQBox boundbox;
	private List<HAWQPoint> points;

	/**
	 * Initialize a polygon from string
	 * 
	 * @param value
	 *            the value that polygon init from. Should be like this:
	 *            ((3.3,3.4),(4.3,4.4),(5.4,5.5))
	 * @throws HAWQException
	 */
	public HAWQPolygon(String value) throws HAWQException
	{
		value = value.replaceAll(" ", "");
		String[] pointStrs = value.substring(1, value.length() - 1).split(",");
		if (pointStrs.length % 2 != 0)
			throw new HAWQException("Cannot convert " + value
					+ " to HAWQPolygon");

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
		init(points, null);
	}

	/**
	 * Initialize a polygon from vertexes and bound box
	 * 
	 * @param points
	 * @param boundbox
	 */
	public HAWQPolygon(List<HAWQPoint> points, HAWQBox boundbox)
	{
		init(points, boundbox);
	}

	/**
	 * Initialize a polygon from vertexes
	 * 
	 * @param points
	 */
	public HAWQPolygon(List<HAWQPoint> points)
	{
		this(points, null);
	}

	private void init(List<HAWQPoint> points, HAWQBox boundbox)
	{
		this.points = points;
		this.boundbox = boundbox;
		if (this.boundbox == null)
			initBoundbox();
	}

	private void initBoundbox()
	{
		int pointsNum = points.size();
		double minX = Double.MAX_VALUE;
		double minY = Double.MAX_VALUE;
		double maxX = Double.MIN_VALUE;
		double maxY = Double.MIN_VALUE;
		for (int i = 0; i < pointsNum; ++i)
		{
			double pointX = points.get(i).getX();
			double pointY = points.get(i).getY();
			if (pointX < minX)
				minX = pointX;
			else if (pointX > maxX)
				maxX = pointX;
			if (pointY < minY)
				minY = pointY;
			else if (pointY > maxY)
				maxY = pointY;
		}
		this.boundbox = new HAWQBox(maxX, maxY, minX, minY);
	}

	/**
	 * Get bound box of this path
	 * 
	 * @return bound box
	 */
	public HAWQBox getBoundbox()
	{
		return boundbox;
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

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQPolygon)
		{
			HAWQPolygon other = (HAWQPolygon) obj;
			if (points.size() != other.getPoints().size())
				return false;

			for (int i = 0; i < points.size(); i++)
			{
				if (!points.get(i).equals(other.getPoints().get(i)))
					return false;
			}
			return boundbox.equals(other.getBoundbox());
		}
		return false;
	}

	@Override
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append('(');
		int numOfPoints = points.size();
		for (int i = 0; i < numOfPoints; ++i)
		{
			buffer.append(points.get(i));
			if (i != numOfPoints - 1)
				buffer.append(',');
		}
		buffer.append(')');
		return buffer.toString();
	}
}
