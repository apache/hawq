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


import com.pivotal.hawq.mapreduce.metadata.HAWQTableFormat;

import java.util.List;

/**
 * Definition of a target HAWQ table to test on.
 */
public class HAWQTable {
	private static final int DEFALUT_BLOCKSIZE = 32768; // 32K
	private static final int DEFAULT_ROWGROUPSIZE = 8388608; // 8M
	private static final int DEFAULT_PAGESIZE = 1048576; // 1M

	// properties related to table creation
	private final String tableName;
	private final HAWQTableFormat tableStorage;
	private final List<String> columnTypes;
	private final String compressType;
	private final int compressLevel;
	private final int blockSize;
	private final int rowGroupSize;
	private final int pageSize;
	private final boolean checkSum;

	// data generation strategy
	private DataProvider dataProvider;

	private HAWQTable(Builder builder) {
		this.tableName		= builder.tableName;
		this.tableStorage	= builder.tableStorage;
		this.columnTypes	= builder.columnTypes;
		this.compressType	= builder.compressType;
		this.compressLevel	= builder.compressLevel;
		this.blockSize 		= builder.blockSize;
		this.rowGroupSize	= builder.rowGroupSize;
		this.pageSize 		= builder.pageSize;
		this.checkSum 		= builder.checkSum;
		this.dataProvider	= builder.dataProvider;

		if (tableStorage != HAWQTableFormat.AO && tableStorage != HAWQTableFormat.Parquet)
			throw new AssertionError("invalid table format: " + tableStorage);
	}

	public static class Builder {
		private final String tableName;
		private HAWQTableFormat tableStorage = HAWQTableFormat.AO;
		private final List<String> columnTypes;
		private String compressType	= "none";
		private int compressLevel	= 0;
		private int blockSize		= DEFALUT_BLOCKSIZE;
		private int rowGroupSize	= DEFAULT_ROWGROUPSIZE;
		private int pageSize		= DEFAULT_PAGESIZE;
		private boolean checkSum	= false;

		private DataProvider dataProvider = DataProvider.ENUMERATE;

		public Builder(String tableName, List<String> columnTypes) {
			this.tableName = tableName;
			this.columnTypes = columnTypes;
		}

		public Builder storage(HAWQTableFormat tableStorage) {
			this.tableStorage = tableStorage;
			return this;
		}

		public Builder compress(String compressType, int compressLevel) {
			this.compressType = compressType;
			this.compressLevel = compressLevel;
			return this;
		}

		public Builder blockSize(int blockSize) {
			this.blockSize = blockSize;
			return this;
		}

		public Builder rowGroupSize(int rowGroupSize) {
			this.rowGroupSize = rowGroupSize;
			return this;
		}

		public Builder pageSize(int pageSize) {
			this.pageSize = pageSize;
			return this;
		}

		public Builder checkSum(boolean checkSum) {
			this.checkSum = checkSum;
			return this;
		}

		public Builder provider(DataProvider provider) {
			this.dataProvider = provider;
			return this;
		}

		public HAWQTable build() {
			return new HAWQTable(this);
		}
	}

	// Generate create table DDL.
	public String generateDDL() {
		StringBuilder columnList = new StringBuilder("c0 " + columnTypes.get(0));
		for (int i = 1; i < columnTypes.size(); ++i) {
			columnList.append(", c").append(i).append(" ").append(columnTypes.get(i));
		}

		StringBuilder withClause = new StringBuilder();
		if (tableStorage == HAWQTableFormat.AO) {
			withClause.append("appendonly=true, orientation=row")
					  .append(", compresstype=").append(compressType)
					  .append(", blocksize=").append(blockSize)
					  .append(", checksum=").append(checkSum);
		} else {
			withClause.append("appendonly=true, orientation=parquet")
					  .append(", compresstype=").append(compressType)
					  .append(", rowgroupsize=").append(rowGroupSize)
					  .append(", pagesize=").append(pageSize);
		}
		// snappy does not support compresslevel option
		if (!compressType.equalsIgnoreCase("snappy")) {
			withClause.append(", compresslevel=").append(compressLevel);
		}

		return String.format("DROP TABLE IF EXISTS %s;\n", tableName) +
				String.format("CREATE TABLE %s (%s) with (%s);\n", tableName, columnList, withClause);
	}

	// Generate SQLs for inserting data.
	public String generateData() {
		return dataProvider.getInsertSQLs(this);
	}

	public String getTableName() {
		return tableName;
	}

	public List<String> getColumnTypes() {
		return columnTypes;
	}
}
