package com.pivotal.hawq.mapreduce.demo;

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


import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * A mapper which echos each field of {@link com.pivotal.hawq.mapreduce.HAWQRecord} separated by ','.
 */
public class HAWQEchoMapper extends Mapper<Void, HAWQRecord, Text, Text> {
	@Override
	protected void map(Void key, HAWQRecord value, Context context) throws IOException, InterruptedException {
		try {
			StringBuffer buf = new StringBuffer(value.getString(1));
			for (int i = 2; i <= value.getSchema().getFieldCount(); i++) {
				buf.append(",").append(value.getString(i));
			}
			context.write(new Text(buf.toString()), null);

		} catch (HAWQException e) {
			throw new IOException(e);
		}
	}
}
