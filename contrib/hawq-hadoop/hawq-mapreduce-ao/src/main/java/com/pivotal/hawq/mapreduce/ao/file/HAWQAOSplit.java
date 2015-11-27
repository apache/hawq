package com.pivotal.hawq.mapreduce.ao.file;

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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A section of an input file. Returned by
 * HAWQAOInputFormat.getSplits(JobContext) and passed to
 * HAWQAOInputFormat.createRecordReader(InputSplit,TaskAttemptContext).
 */
public class HAWQAOSplit extends FileSplit
{
	private boolean checksum;
	private String compressType = null;
	private int blockSize;

	public HAWQAOSplit()
	{
		super();
	}

	/**
	 * Constructs a split with host information
	 * 
	 * @param file
	 *            the file name
	 * @param start
	 *            the position of the first byte in the file to process
	 * @param length
	 *            the number of bytes in the file to process
	 * @param hosts
	 *            the list of hosts containing the block, possibly null
	 */
	public HAWQAOSplit(Path file, long start, long length, String[] hosts,
			boolean checksum, String compressType, int blockSize)
	{
		super(file, start, length, hosts);
		this.checksum = checksum;
		this.compressType = compressType;
		this.blockSize = blockSize;
	}

	@Override
	public String toString()
	{
		return super.toString() + "+" + checksum + "+" + compressType + "+"
				+ blockSize;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		super.write(out);
		out.writeBoolean(checksum);
		Text.writeString(out, compressType);
		out.writeInt(blockSize);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		super.readFields(in);
		checksum = in.readBoolean();
		compressType = Text.readString(in);
		blockSize = in.readInt();
	}

	public boolean getChecksum()
	{
		return checksum;
	}

	public String getCompressType()
	{
		return compressType;
	}

	public int getBlockSize()
	{
		return blockSize;
	}
}
