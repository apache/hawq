package org.apache.hawq.pxf.plugins.json;

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

import static org.apache.commons.lang.StringUtils.isEmpty;

import java.io.IOException;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hdfs.HdfsSplittableDataAccessor;

/**
 * This JSON accessor for PXF will read JSON data and pass it to a {@link JsonResolver}.
 * 
 * This accessor supports a single JSON record per line, or a multi-line JSON records if the <b>IDENTIFIER</b> parameter
 * is set.
 * 
 * When provided the <b>IDENTIFIER</b> indicates the member name used to determine the encapsulating json object to
 * return.
 */
public class JsonAccessor extends HdfsSplittableDataAccessor {

	public static final String IDENTIFIER_PARAM = "IDENTIFIER";
	public static final String RECORD_MAX_LENGTH_PARAM = "MAXLENGTH";

	/**
	 * If provided indicates the member name which will be used to determine the encapsulating json object to return.
	 */
	private String identifier = "";

	/**
	 * Optional parameter that allows to define the max length of a json record. Records that exceed the allowed length
	 * are skipped. This parameter is applied only for the multi-line json records (e.g. when the IDENTIFIER is
	 * provided).
	 */
	private int maxRecordLength = Integer.MAX_VALUE;

	public JsonAccessor(InputData inputData) throws Exception {
		// Because HdfsSplittableDataAccessor doesn't use the InputFormat we set it to null.
		super(inputData, null);

		if (!isEmpty(inputData.getUserProperty(IDENTIFIER_PARAM))) {

			identifier = inputData.getUserProperty(IDENTIFIER_PARAM);

			// If the member identifier is set then check if a record max length is defined as well.
			if (!isEmpty(inputData.getUserProperty(RECORD_MAX_LENGTH_PARAM))) {
				maxRecordLength = Integer.valueOf(inputData.getUserProperty(RECORD_MAX_LENGTH_PARAM));
			}
		}
	}

	@Override
	protected Object getReader(JobConf conf, InputSplit split) throws IOException {
		if (!isEmpty(identifier)) {
			conf.set(JsonRecordReader.RECORD_MEMBER_IDENTIFIER, identifier);
			conf.setInt(JsonRecordReader.RECORD_MAX_LENGTH, maxRecordLength);
			return new JsonRecordReader(conf, (FileSplit) split);
		} else {
			return new LineRecordReader(conf, (FileSplit) split);
		}
	}
}