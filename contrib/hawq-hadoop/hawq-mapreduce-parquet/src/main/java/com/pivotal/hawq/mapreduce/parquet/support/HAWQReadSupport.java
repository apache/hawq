package com.pivotal.hawq.mapreduce.parquet.support;

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
import com.pivotal.hawq.mapreduce.parquet.convert.HAWQRecordMaterializer;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import java.util.Map;

/**
 * User: gaod1
 * Date: 8/8/13
 */
public class HAWQReadSupport extends ReadSupport<HAWQRecord> {

	private static final String KEY_HAWQ_SCHEMA = "hawq.schema";
	private static final String HAWQ_REQUESTED_SCHEMA = "hawq.schema.requested";

	@Override
	public ReadContext init(Configuration configuration,
							Map<String, String> keyValueMetaData,
							MessageType fileSchema) {

//		String requestedProjectionString = configuration.get(HAWQ_REQUESTED_SCHEMA);
//
//		if (requestedProjectionString == null) { // read all data
//			return new ReadContext(fileSchema);
//		}
//
//		HAWQSchema requestedHAWQSchema = HAWQSchema.fromString(requestedProjectionString);
//		MessageType requestedSchema = HAWQSchemaConverter.convertToParquet(requestedHAWQSchema);
//		return new ReadContext(requestedSchema);

		return new ReadContext(fileSchema);
	}

	@Override
	public RecordMaterializer<HAWQRecord> prepareForRead(Configuration configuration,
														 Map<String, String> keyValueMetaData,
														 MessageType fileSchema, ReadContext readContext) {

		HAWQSchema hawqSchema = HAWQSchema.fromString(keyValueMetaData.get(KEY_HAWQ_SCHEMA));
		return new HAWQRecordMaterializer(
				readContext.getRequestedSchema(), // requested parquet schema
				hawqSchema); // corresponding requested HAWQSchema
	}
}
