package org.greenplum.pxf.s3;

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

import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.net.InetAddress;
import java.util.List;

/**
 * Fragmenter for S3
 * 
 * Based on the input, a bucket name plus a prefix, expands that to a list of
 * matching paths, which is returned by getFragments()
 */
public class S3Fragmenter extends Fragmenter {

	private static final Log LOG = LogFactory.getLog(S3Fragmenter.class);
	private final PxfS3 pxfS3;

	public S3Fragmenter(InputData inputData) {
		super(inputData);
		LOG.info("dataSource: " + inputData.getDataSource());
		pxfS3 = PxfS3.fromInputData(inputData);
	}
	
	/**
	 * This is for tests
	 * @param inputData
	 * @param pxfS3 for tests, this would be a PxfS3Mock instance
	 */
	protected S3Fragmenter(InputData inputData, PxfS3 pxfS3) {
		super(inputData);
		this.pxfS3 = pxfS3;
	}

	/**
	 * Returns a list of distinct S3 objects, including the full path to each, based
	 * on the bucket name plus prefix passed in via the InputData.
	 */
	@Override
	public List<Fragment> getFragments() throws Exception {
		String pxfHost = InetAddress.getLocalHost().getHostAddress();
		String[] hosts = new String[] { pxfHost };
		for (String key : pxfS3.getKeysInBucket(super.inputData)) {
			// Example key values: test-write/1524148597-0000000911_1, test-write/1524496731-0000002514_5
			fragments.add(new Fragment(inputData.getDataSource(), hosts, key.getBytes()));
		}
		return fragments;
	}

}
