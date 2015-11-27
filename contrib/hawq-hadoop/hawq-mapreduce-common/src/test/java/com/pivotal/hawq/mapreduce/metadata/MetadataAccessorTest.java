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


import junit.framework.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;

public class MetadataAccessorTest {

	@Test
	public void testInvalidMetadataFile() throws Exception {
		try {
			MetadataAccessor.newInstanceUsingFile("no_such_file");
			Assert.fail();
		} catch (MetadataAccessException e) {
			Assert.assertSame(e.getCause().getClass(), FileNotFoundException.class);
		}

		try {
			MetadataAccessor.newInstanceUsingFile(
					getFilePathUnderClasspath("/invalid_metadata.yaml"));
			Assert.fail();
		} catch (MetadataAccessException e) {}
	}

	@Test
	public void testIncorrectAPICall() throws Exception {
		MetadataAccessor accessor;

		accessor = MetadataAccessor.newInstanceUsingFile(
				getFilePathUnderClasspath("/sample_ao_metadata.yaml"));
		Assert.assertEquals(HAWQTableFormat.AO, accessor.getTableFormat());
		try {
			accessor.getParquetMetadata();
			Assert.fail();
		} catch (IllegalStateException e) {
			Assert.assertEquals("shouldn't call getParquetMetadata on a AO table!", e.getMessage());
		}

		accessor = MetadataAccessor.newInstanceUsingFile(
				getFilePathUnderClasspath("/sample_parquet_metadata.yaml"));
		Assert.assertEquals(HAWQTableFormat.Parquet, accessor.getTableFormat());
		try {
			accessor.getAOMetadata();
			Assert.fail();
		} catch (IllegalStateException e) {
			Assert.assertEquals("shouldn't call getAOMetadata on a Parquet table!", e.getMessage());
		}
	}

	private String getFilePathUnderClasspath(String filename) {
		return getClass().getResource(filename).getPath();
	}
}
