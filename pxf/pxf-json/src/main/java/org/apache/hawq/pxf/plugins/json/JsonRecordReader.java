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

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hawq.pxf.plugins.json.parser.PartitionedJsonParser;

/**
 * Multi-line json object reader. JsonRecordReader uses a member name (set by the <b>IDENTIFIER</b> PXF parameter) to
 * determine the encapsulating object to extract and read.
 * 
 * JsonRecordReader supports compressed input files as well.
 * 
 * As a safe guard set the optional <b>MAXLENGTH</b> parameter to limit the max size of a record.
 */
public class JsonRecordReader implements RecordReader<LongWritable, Text> {

	private static final Log LOG = LogFactory.getLog(JsonRecordReader.class);

	public static final String RECORD_MEMBER_IDENTIFIER = "json.input.format.record.identifier";
	public static final String RECORD_MAX_LENGTH = "multilinejsonrecordreader.maxlength";

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private int maxObjectLength;
	private InputStream is;
	private PartitionedJsonParser parser;
	private final String jsonMemberName;

	/**
	 * Create new multi-line json object reader.
	 * 
	 * @param conf
	 *            Hadoop context
	 * @param split
	 *            HDFS split to start the reading from
	 * @throws IOException
	 */
	public JsonRecordReader(JobConf conf, FileSplit split) throws IOException {

		this.jsonMemberName = conf.get(RECORD_MEMBER_IDENTIFIER);
		this.maxObjectLength = conf.getInt(RECORD_MAX_LENGTH, Integer.MAX_VALUE);

		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(conf);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(split.getPath());
		if (codec != null) {
			is = codec.createInputStream(fileIn);
			start = 0;
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				fileIn.seek(start);
			}
			is = fileIn;
		}
		parser = new PartitionedJsonParser(is);
		this.pos = start;
	}

	/*
	 * {@inheritDoc}
	 */
	@Override
	public boolean next(LongWritable key, Text value) throws IOException {

		while (pos < end) {

			String json = parser.nextObjectContainingMember(jsonMemberName);
			pos = start + parser.getBytesRead();

			if (json == null) {
				return false;
			}

			long jsonStart = pos - json.length();

			// if the "begin-object" position is after the end of our split, we should ignore it
			if (jsonStart >= end) {
				return false;
			}

			if (json.length() > maxObjectLength) {
				LOG.warn("Skipped JSON object of size " + json.length() + " at pos " + jsonStart);
			} else {
				key.set(jsonStart);
				value.set(json);
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

	@Override
	public long getPos() throws IOException {
		return pos;
	}

	/*
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void close() throws IOException {
		if (is != null) {
			is.close();
		}
	}

	/*
	 * {@inheritDoc}
	 */
	@Override
	public float getProgress() throws IOException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}
}
