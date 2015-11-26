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

import static com.pivotal.hawq.mapreduce.MRFormatConfiguration.TEST_DB_NAME;
import static com.pivotal.hawq.mapreduce.MRFormatConfiguration.TEST_DB_PORT;

public abstract class TPCHTester {

	public static class HAWQTPCHSpec {
		private final String scale;
		private final HAWQTableFormat tableFormat;
		private final boolean isPartition;

		private String loadCmd;

		public HAWQTPCHSpec(String scale,
							HAWQTableFormat tableFormat,
							boolean isPartition) {
			this.scale = scale;
			this.tableFormat = tableFormat;
			this.isPartition = isPartition;
		}

		public String getLoadCmd(int segmentNumber) {
			if (loadCmd == null) {
				StringBuilder buf = new StringBuilder("./generate_load_tpch.pl");
				buf.append(" -scale ").append(scale)
				   .append(" -num ").append(segmentNumber)
				   .append(" -port ").append(TEST_DB_PORT)
				   .append(" -db ").append(TEST_DB_NAME)
				   .append(" -table ao")
				   .append(" -orient ").append(tableFormat.getOrientation())
				   .append(" -partition ").append(isPartition)
				   .append(" -dbversion hawq")
				   .append(" -compress false");

				loadCmd = buf.toString();
			}
			return loadCmd;
		}

		@Override
		public String toString() {
			return String.format("tpch_%s_%s_%s",
								 scale,
								 tableFormat.toString().toLowerCase(),
								 isPartition ? "part" : "nopart");
		}
	}

	/**
	 * Generate TPCH data and test a TPCH table, typically "lineitem".
	 * @param tpchSpec specification of a TPCH load
	 * @param tableName table to read
	 * @throws Exception
	 */
	protected abstract void testTPCHTable(
			HAWQTPCHSpec tpchSpec, String tableName) throws Exception;
}
