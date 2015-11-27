package com.pivotal.hawq.mapreduce.file;

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


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HAWQAOFileStatusTest {

	@Test
	public void testHAWQAOFileStatusStringLongBooleanStringInt() {
		String pathStr = "/gpsql/gpseg0/16385/17673/17709.1";
		long fileLength = 979982384;
		boolean checksum = false;
		String compressType = "none";
		int blockSize = 32768;
		HAWQAOFileStatus aofilestatus = new HAWQAOFileStatus(pathStr,
				fileLength, checksum, compressType, blockSize);
		assertFalse(aofilestatus.getChecksum());
		assertEquals(aofilestatus.getBlockSize(), blockSize);
		assertEquals(aofilestatus.getCompressType(), compressType);
		assertEquals(aofilestatus.getFileLength(), fileLength);
		assertEquals(aofilestatus.getFilePath(), pathStr);
		assertEquals(aofilestatus.toString(), pathStr + "|" + fileLength + "|"
				+ checksum + "|" + compressType + "|" + blockSize);
	}

}
