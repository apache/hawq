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


import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;

/**
 * Store value of array (_int2, _int4, etc.) in database
 */
public final class HAWQArray implements Array
{

	StringBuffer buffer = new StringBuffer();
	private int baseType = -1;
	private String baseTypeName = "";
	private Object array = null;

	/**
	 * @param baseType
	 *            type oid, e.g. 23 means int4
	 * @param baseTypeName
	 *            type name, e.g. int4
	 * @param startOfEachDime
	 * @param numOfEachDime
	 * @param datas
	 */
	public HAWQArray(int baseType,
			HAWQPrimitiveField.PrimitiveType baseTypeName,
			int[] startOfEachDime, int[] numOfEachDime, Object datas)
	{
		String separator = ",";
		if (baseTypeName == HAWQPrimitiveField.PrimitiveType.BOX)
			separator = ";";

		boolean startAllOne = true;
		for (int i = 0; i < startOfEachDime.length; ++i)
		{
			if (startOfEachDime[i] != 1)
			{
				startAllOne = false;
				break;
			}
		}
		if (!startAllOne)
		{
			for (int j = 0; j < startOfEachDime.length; ++j)
			{
				buffer.append('[').append(startOfEachDime[j]).append(':')
						.append((startOfEachDime[j] + numOfEachDime[j] - 1))
						.append(']');
			}
			buffer.append('=');
		}

		Object allDatas[] = null;
		switch (startOfEachDime.length)
		{
		case 1:
			this.array = datas;
			buffer.append('{');
			allDatas = (Object[]) datas;
			for (int i = 0; i < allDatas.length; ++i)
			{
				buffer.append(allDatas[i]);
				if (i != allDatas.length - 1)
					buffer.append(separator);
				else
					buffer.append('}');
			}
			break;
		case 2:
			Object newDatas2[][] = new Object[numOfEachDime[0]][numOfEachDime[1]];
			buffer.append('{');
			allDatas = (Object[]) datas;
			for (int i = 0; i < newDatas2.length; ++i)
			{
				buffer.append('{');
				for (int j = 0; j < newDatas2[i].length; ++j)
				{
					newDatas2[i][j] = allDatas[i * newDatas2[i].length + j];
					buffer.append(newDatas2[i][j]);
					if (j != newDatas2[i].length - 1)
						buffer.append(separator);
					else
						buffer.append('}');
				}
				if (i != newDatas2.length - 1)
					buffer.append(',');
				else
					buffer.append('}');
			}
			array = newDatas2;
			break;
		case 3:
			Object newDatas3[][][] = new Object[numOfEachDime[0]][numOfEachDime[1]][numOfEachDime[2]];
			buffer.append('{');
			allDatas = (Object[]) datas;
			for (int i = 0; i < newDatas3.length; ++i)
			{
				buffer.append('{');
				for (int j = 0; j < newDatas3[i].length; ++j)
				{
					buffer.append('{');
					for (int k = 0; k < newDatas3[i][j].length; ++k)
					{
						newDatas3[i][j][k] = allDatas[i * newDatas3[i].length
								* newDatas3[i][j].length + j
								* newDatas3[i][j].length + k];
						buffer.append(newDatas3[i][j][k]);
						if (k != newDatas3[i][j].length - 1)
							buffer.append(separator);
						else
							buffer.append('}');
					}
					if (j != newDatas3[i].length - 1)
						buffer.append(',');
					else
						buffer.append('}');
				}
				if (i != newDatas3.length - 1)
					buffer.append(',');
				else
					buffer.append('}');
			}
			array = newDatas3;
			break;
		}

		this.baseType = baseType;
		this.baseTypeName = baseTypeName.toString().toLowerCase();
	}

	@Override
	public String toString()
	{
		return buffer.toString();
	}

	public void free() throws SQLException
	{
		array = null;
	}

	public Object getArray() throws SQLException
	{
		return array;
	}

	public Object getArray(Map<String, Class<?>> arg0) throws SQLException
	{
		throw new UnsupportedOperationException();
	}

	public Object getArray(long arg0, int arg1) throws SQLException
	{
		throw new UnsupportedOperationException();
	}

	public Object getArray(long arg0, int arg1, Map<String, Class<?>> arg2)
			throws SQLException
	{
		throw new UnsupportedOperationException();
	}

	public int getBaseType() throws SQLException
	{
		return baseType;
	}

	public String getBaseTypeName() throws SQLException
	{
		return baseTypeName;
	}

	public ResultSet getResultSet() throws SQLException
	{
		throw new UnsupportedOperationException();
	}

	public ResultSet getResultSet(Map<String, Class<?>> arg0)
			throws SQLException
	{
		throw new UnsupportedOperationException();
	}

	public ResultSet getResultSet(long arg0, int arg1) throws SQLException
	{
		throw new UnsupportedOperationException();
	}

	public ResultSet getResultSet(long arg0, int arg1,
			Map<String, Class<?>> arg2) throws SQLException
	{
		throw new UnsupportedOperationException();
	}

}
