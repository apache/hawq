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


/**
 * Extends {@link HAWQFileStatus} and add particular attribute of file for
 * append only files.
 */
public final class HAWQAOFileStatus extends HAWQFileStatus
{
	private boolean checksum;
	private String compressType = null;
	private int blockSize;

	/**
	 * Constructor
	 * 
	 * @param filePath path
	 * @param fileLength file length
	 * @param checksum checksum
	 * @param compressType compress type, acceptable values are none | zlib | quicklz
	 * @param blockSize block size
	 */
	public HAWQAOFileStatus(String filePath, long fileLength, boolean checksum,
			String compressType, int blockSize)
	{
		super(filePath, fileLength);
		this.checksum = checksum;
		this.compressType = compressType;
		this.blockSize = blockSize;

		if (!("none".equals(compressType) || "zlib".equals(compressType) || "quicklz".equals(compressType)))
			throw new UnsupportedOperationException("CompessType " + compressType + " is not supported");
	}

	/**
	 * Get checksum of this file
	 * 
	 * @return checksum of this file
	 */
	public boolean getChecksum()
	{
		return checksum;
	}

	/**
	 * Get compress type of this file
	 * 
	 * @return compress type of this file
	 */
	public String getCompressType()
	{
		return compressType;
	}

	/**
	 * Get ao block size of this file
	 * 
	 * @return block size of this file
	 */
	public int getBlockSize()
	{
		return blockSize;
	}

	@Override
	public String toString()
	{
		return filePath + "|" + fileLength + "|" + checksum + "|" + compressType + "|" + blockSize;
	}
}
