package com.pivotal.hawq.mapreduce.conf;

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


import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.apache.hadoop.conf.Configuration;

/**
 * A container for configuration property names for jobs with HAWQ input/output.
 * <p/>
 * The job can be configured using the static methods in this class.
 * Alternatively, the properties can be set in the configuration with proper
 * values.
 */
public final class HAWQConfiguration {

	/**
	 * The schema of the table
	 */
	public static final String TABLE_SCHEMA_PROPERTY = "mapreduce.hawq.table.schema";

	/**
	 * The encoding of the table
	 */
	public static final String TABLE_ENCODING_PROPERTY = "mapreduce.hawq.table.encoding";

	/*
	 * GOSQL-1047
	 * 
	 * Save version of database
	 */
	/**
	 * The version of the database
	 */
	public static final String DATABASE_VERSION_PROPERTY = "mapreduce.hawq.database.version";

	/**
	 * Get the schema of the table from configuration
	 *
	 * @param conf the configuration
	 * @return schema of the table
	 */
	public static HAWQSchema getInputTableSchema(Configuration conf) {
		return HAWQSchema.fromString(conf.get(TABLE_SCHEMA_PROPERTY, null));
	}

	/**
	 * Set the schema of the table into configuration
	 *
	 * @param conf   the configuration
	 * @param schema schema of the table
	 */
	public static void setInputTableSchema(Configuration conf, HAWQSchema schema) {
		conf.setStrings(TABLE_SCHEMA_PROPERTY, schema.toString());
	}

	/**
	 * Get the encoding of the table from configuration
	 *
	 * @param conf the configuration
	 * @return encoding of the table
	 */
	public static String getInputTableEncoding(Configuration conf) {
		return conf.get(TABLE_ENCODING_PROPERTY, "UTF8");
	}

	/**
	 * Set the encoding of the table into configuration
	 *
	 * @param conf     the configuration
	 * @param encoding encoding of the table
	 */
	public static void setInputTableEncoding(Configuration conf, String encoding) {
		conf.set(TABLE_ENCODING_PROPERTY, encoding);
	}

	/*
	 * GOSQL-1047
	 * 
	 * Two functions below supply get/set method for version of database
	 */

	/**
	 * Get version of database from configuration
	 *
	 * @param conf The configuration
	 * @return version of database
	 */
	public static String getDatabaseVersion(Configuration conf) {
		return conf.get(HAWQConfiguration.DATABASE_VERSION_PROPERTY);
	}

	/**
	 * Set version of database into configuration
	 *
	 * @param conf    the configuration
	 * @param version version of database
	 */
	public static void setDatabaseVersion(Configuration conf, String version) {
		conf.set(HAWQConfiguration.DATABASE_VERSION_PROPERTY, version);
	}

}
