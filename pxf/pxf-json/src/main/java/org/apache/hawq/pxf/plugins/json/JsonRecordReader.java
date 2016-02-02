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

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.security.InvalidParameterException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hawq.pxf.api.Fragment;

/**
 * {@link RecordReader} implementation that can read multiline JSON objects.
 */
public class JsonRecordReader implements RecordReader<LongWritable, Text> {

	private static final Log LOG = LogFactory.getLog(JsonRecordReader.class);

	public static final String RECORD_IDENTIFIER = "json.input.format.record.identifier";

	private JsonStreamReader streamReader = null;
	private long start = 0, end = 0;
	private float toRead = 0;
	private String identifier = null;

	/**
	 * @param conf
	 *            Hadoop context
	 * @param split
	 *            HDFS Split as defined by the {@link Fragment}.
	 * @throws IOException
	 */
	public JsonRecordReader(JobConf conf, FileSplit split) throws IOException {
		LOG.debug("Conf is " + conf + ". Split is " + split);

		this.identifier = conf.get(RECORD_IDENTIFIER);

		if (isEmpty(this.identifier)) {
			throw new InvalidParameterException("The IDENTIFIER parameter is not set.");
		} else {
			LOG.debug("Initializing JsonRecordReader with identifier " + identifier);
		}

		// get relevant data
		Path file = split.getPath();

		LOG.debug("File is " + file);

		start = split.getStart();
		end = start + split.getLength();
		toRead = end - start;

		LOG.debug("FileSystem is " + FileSystem.get(conf));

		FSDataInputStream strm = FileSystem.get(conf).open(file);

		if (start != 0) {
			strm.seek(start);
		}

		streamReader = new JsonStreamReader(identifier, new BufferedInputStream(strm));
	}

	/*
	 * {@inheritDoc}
	 */
	@Override
	public boolean next(LongWritable key, Text value) throws IOException {

		// Exit condition (end of block/file)
		if (streamReader.getBytesRead() < (end - start)) {

			String record = streamReader.getJsonRecord();
			if (record != null) {
				key.set(streamReader.getBytesRead());
				value.set(record);
				return true;
			}
		}

		return false;
	}

	/*
	 * {@inheritDoc}
	 */
	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	/*
	 * {@inheritDoc}
	 */
	@Override
	public Text createValue() {
		return new Text();
	}

	/*
	 * {@inheritDoc}
	 */
	@Override
	public long getPos() throws IOException {
		return start + streamReader.getBytesRead();
	}

	/*
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		streamReader.close();
	}

	/*
	 * {@inheritDoc}
	 */
	@Override
	public float getProgress() throws IOException {
		return (float) streamReader.getBytesRead() / toRead;
	}
}