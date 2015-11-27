package com.pivotal.hawq.mapreduce.metadata;

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


/**
 * Represent common information about a HAWQ array.
 */
class HAWQDatabaseMetadata {
	protected String version;
	protected String encoding;
	protected String dfsURL;

	public HAWQDatabaseMetadata(String version, String encoding, String dfsURL) {
		this.version = version;
		this.encoding = encoding;
		this.dfsURL = dfsURL;
	}

	public String getVersion() {
		return version;
	}

	public String getEncoding() {
		return encoding;
	}

	public String getDfsURL() {
		return dfsURL;
	}
}
