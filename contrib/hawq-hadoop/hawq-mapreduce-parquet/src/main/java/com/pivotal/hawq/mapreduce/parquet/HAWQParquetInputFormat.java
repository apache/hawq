package com.pivotal.hawq.mapreduce.parquet;

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


import com.google.common.collect.Lists;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.file.HAWQFileStatus;
import com.pivotal.hawq.mapreduce.metadata.HAWQParquetTableMetadata;
import com.pivotal.hawq.mapreduce.parquet.support.HAWQReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import parquet.hadoop.ParquetInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * An InputFormat that reads input data from HAWQ Parquet table.
 */
public class HAWQParquetInputFormat extends ParquetInputFormat<HAWQRecord> {

	private static HAWQFileStatus[] hawqFileStatuses;

	public HAWQParquetInputFormat() {
		super(HAWQReadSupport.class);
	}

	public static void setInput(Configuration conf, HAWQParquetTableMetadata metadata) {
		hawqFileStatuses = metadata.getFileStatuses();
	}

	@Override
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
		if (hawqFileStatuses == null) {
			throw new IllegalStateException("Please call HAWQParquetInputFormat.setInput first!");
		}

		if (hawqFileStatuses.length == 0) {
			return Lists.newArrayList(); // handle empty table
		}

		return super.getSplits(jobContext);
	}

	@Override
	protected List<FileStatus> listStatus(JobContext jobContext) throws IOException {
		List<FileStatus> result = Lists.newArrayList();
		for (HAWQFileStatus hawqFileStatus : hawqFileStatuses) {
			if (hawqFileStatus.getFileLength() == 0) continue; // skip empty file

			Path path = new Path(hawqFileStatus.getFilePath());
			FileSystem fs = path.getFileSystem(jobContext.getConfiguration());
			FileStatus dfsStat = fs.getFileStatus(path);
			// rewrite file length because HAWQ records the logicalEOF of file, which may
			// be smaller than the file's actual EOF
			FileStatus hawqStat = new FileStatus(
					hawqFileStatus.getFileLength(), // rewrite to logicalEOF
					dfsStat.isDirectory(),
					dfsStat.getReplication(),
					dfsStat.getBlockSize(),
					dfsStat.getModificationTime(),
					dfsStat.getAccessTime(),
					dfsStat.getPermission(),
					dfsStat.getOwner(),
					dfsStat.getGroup(),
					dfsStat.getPath());
			result.add(hawqStat);
		}

		return result;
	}
}
