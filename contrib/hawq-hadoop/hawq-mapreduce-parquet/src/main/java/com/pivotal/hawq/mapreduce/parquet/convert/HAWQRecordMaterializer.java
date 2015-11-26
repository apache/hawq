package com.pivotal.hawq.mapreduce.parquet.convert;

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
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

/**
 * HAWQ's implementation of the RecordMaterializer interface.
 *
 * <p>This class materialize HAWQRecord objects from a stream of Parquet data,
 * using a HAWQRecordConverter internally to convert data types between HAWQ and Parquet.
 *
 */
public class HAWQRecordMaterializer extends RecordMaterializer<HAWQRecord> {

	private HAWQRecordConverter rootConverter;

	public HAWQRecordMaterializer(MessageType requestedSchema, HAWQSchema hawqSchema) {
		rootConverter = new HAWQRecordConverter(requestedSchema, hawqSchema);
	}

	@Override
	public HAWQRecord getCurrentRecord() {
		return rootConverter.getCurrentRecord();
	}

	@Override
	public GroupConverter getRootConverter() {
		return rootConverter;
	}
}
