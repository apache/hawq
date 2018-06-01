package org.apache.hawq.pxf.plugins.s3;

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

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class S3ParquetJsonAccessor extends Plugin implements ReadAccessor {

	private static final Log LOG = LogFactory.getLog(S3ParquetJsonAccessor.class);

	private PxfS3 pxfS3;
	private ParquetReader<SimpleRecord> reader;
	private JsonRecordFormatter.JsonGroupFormatter formatter;

	public S3ParquetJsonAccessor(InputData inputData) {
		super(inputData);
		pxfS3 = PxfS3.fromInputData(inputData);
		pxfS3.setObjectName(new String(inputData.getFragmentMetadata()));
	}

	// Called once per object in S3
	@Override
	public boolean openForRead() throws Exception {
		LOG.info("openForRead(): " + pxfS3);
		Path path = new Path(pxfS3.getS3aURI());
		Configuration conf = new Configuration();
		ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
		this.reader = ParquetReader.builder(new SimpleReadSupport(), path).build();
		this.formatter = JsonRecordFormatter.fromSchema(metadata.getFileMetaData().getSchema());
		return true;
	}
	
	@Override
	public OneRow readNextObject() throws Exception {
		OneRow row = null;
		SimpleRecord record = reader.read();
		if (null != record) {
			row = new OneRow(null, formatter.formatRecord(record));
		}
		return row;
	}

	@Override
	public void closeForRead() throws Exception {
		LOG.info("closeForRead()");
		if (null != reader) {
			reader.close();
		}
	}
}
