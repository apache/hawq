package com.pivotal.hawq.mapreduce.parquet;

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


import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.parquet.support.HAWQWriteSupport;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.apache.hadoop.mapreduce.Job;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.util.ContextUtil;

class HAWQParquetOutputFormat extends ParquetOutputFormat<HAWQRecord> {

	private static HAWQSchema hawqSchema;

	public HAWQParquetOutputFormat() {
		super(new HAWQWriteSupport());
	}

	public static void setSchema(Job job, HAWQSchema schema) {
		hawqSchema = schema;
		HAWQWriteSupport.setSchema(ContextUtil.getConfiguration(job), hawqSchema);
	}

	public static HAWQRecord newRecord() {
		if (hawqSchema == null) {
			throw new IllegalStateException("you haven't set HAWQSchema yet");
		}
		return new HAWQRecord(hawqSchema);  // TODO reuse record?
	}
}
