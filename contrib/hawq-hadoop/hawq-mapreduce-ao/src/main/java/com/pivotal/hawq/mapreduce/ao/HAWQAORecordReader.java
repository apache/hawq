package com.pivotal.hawq.mapreduce.ao;

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


import com.pivotal.hawq.mapreduce.conf.HAWQConfiguration;
import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.ao.io.HAWQAOFileReader;
import com.pivotal.hawq.mapreduce.ao.io.HAWQAORecord;

import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * The record reader breaks the data into key/value pairs for input to mapper.
 */
public class HAWQAORecordReader extends RecordReader<Void, HAWQRecord>
{

	private HAWQRecord value = null;
	private HAWQAOFileReader filereader = null;
	private boolean more = true;

	/**
	 * Close the record reader.
	 */
	@Override
	public void close() throws IOException
	{
		filereader.close();
	}

	/**
	 * Get the current key
	 * 
	 * @return the current key or null if there is no current key
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public Void getCurrentKey() throws IOException, InterruptedException
	{
		// Always null
		return null;
	}

	/**
	 * Get the current value.
	 * 
	 * @return the object that was read
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public HAWQRecord getCurrentValue() throws IOException,
			InterruptedException
	{
		return value;
	}

	/**
	 * The current progress of the record reader through its data.
	 * 
	 * @return a number between 0.0 and 1.0 that is the fraction of the data
	 *         read
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException
	{
		return more ? 0f : 100f;
	}

	/**
	 * Called once at initialization.
	 * 
	 * @param split
	 *            the split that defines the range of records to read
	 * @param context
	 *            the information about the task
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException
	{

		// initialize the value
		Configuration conf = context.getConfiguration();

		// Extract the parameters needed by HAWQAOFileReader and HAWQAORecord
		String encoding = HAWQConfiguration.getInputTableEncoding(conf);
		HAWQSchema schema = HAWQConfiguration.getInputTableSchema(conf);
		/*
		 * GPSQL-1047
		 * 
		 * Get version from configuration and init HAWQAORecord with it
		 */
		String version = HAWQConfiguration.getDatabaseVersion(conf);

		filereader = new HAWQAOFileReader(conf, split);

		try
		{
			value = new HAWQAORecord(schema, encoding, version);
		}
		catch (HAWQException hawqE)
		{
			throw new IOException(hawqE.getMessage());
		}
	}

	/**
	 * Read the next key, value pair.
	 * 
	 * @return true if a key/value pair was read
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		try
		{
			if (filereader.readRecord((HAWQAORecord) value))
			{
				return true;
			}
		}
		catch (HAWQException hawqE)
		{
			throw new IOException(hawqE.getMessage());
		}
		more = false;
		return false;
	}
}
