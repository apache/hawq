package com.pivotal.hawq.mapreduce;

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


import java.util.List;

/**
 * A <code>SeriesIntDataProvider</code> that provides
 * series int data for table with single int8 column.
 */
public class SeriesIntDataProvider implements DataProvider {
	private static final int DEFAULT_ROW_NUM = 10000;

	private int rownum;

	public SeriesIntDataProvider() {
		this(DEFAULT_ROW_NUM);
	}

	public SeriesIntDataProvider(int rownum) {
		this.rownum = rownum;
	}

	@Override
	public String getInsertSQLs(HAWQTable table) {
		List<String> colTypes = table.getColumnTypes();
		if (colTypes.size() != 1 || !colTypes.get(0).equals("int8"))
			throw new IllegalArgumentException(
					"SeriesIntDataProvider only accepts table with single int8 column");

		return String.format("INSERT INTO %s SELECT * FROM generate_series(1, %s);",
							 table.getTableName(), rownum);
	}
}
