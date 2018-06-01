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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ReadAccessor which handles delimited TEXT data, passing each line as its own
 * OneRow instance
 */
public class S3TextAccessor extends Plugin implements ReadAccessor {

	private static final Log LOG = LogFactory.getLog(S3TextAccessor.class);
	private PxfS3 pxfS3;

	public S3TextAccessor(InputData metaData) {
		super(metaData);
		pxfS3 = PxfS3.fromInputData(metaData);
		pxfS3.setObjectName(new String(metaData.getFragmentMetadata()));
	}

	@Override
	public boolean openForRead() throws Exception {
		LOG.info("openForRead()");
		pxfS3.open();
		return true;
	}

	@Override
	public OneRow readNextObject() throws Exception {
		OneRow row = null;
		String line = pxfS3.readLine();
		if (null != line) {
			row = new OneRow(null, line);
		}
		return row;
	}

	@Override
	public void closeForRead() throws Exception {
		LOG.info("closeForRead()");
		pxfS3.close();
	}
}
