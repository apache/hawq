package com.pivotal.hawq.mapreduce.ao.io;

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


public class HAWQAOFileReaderTest
{

	private static int printUsage()
	{
		System.out
				.println("HAWQAOFileReaderTest <database_url> <table_name> <whetherToLong>");
		return 2;
	}

	public static void main(String[] args) throws Exception
	{
		/*
		if (args.length != 3)
		{
			System.exit(printUsage());
		}

		String db_url = args[0];
		String table_name = args[1];
		String whetherToLogStr = args[2];

		Metadata metadata = new Metadata(db_url, null, "", table_name);
		if (metadata.getTableType() != Database.TableType.AO_TABLE)
			throw new HAWQException("Only ao is supported");
		HAWQFileStatus[] fileAttributes = metadata.getFileStatus();

		BufferedWriter bw = null;
		boolean whetherToLog = whetherToLogStr.equals("Y");
		if (whetherToLog)
		{
			bw = new BufferedWriter(new FileWriter(table_name + "_test"));
		}

		Configuration conf = new Configuration();
		conf.addResource(new Path(
				"/Users/wangj77/hadoop-2.0.2-alpha-gphd-2.0.1/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path(
				"/home/ioformat/hdfs/etc/hadoop/hdfs-site.xml"));
		conf.reloadConfiguration();

		for (int i = 0; i < fileAttributes.length; i++)
		{
			String pathStr = fileAttributes[i].getFilePath();
			long fileLength = fileAttributes[i].getFileLength();
			HAWQAOFileStatus aofilestatus = (HAWQAOFileStatus) fileAttributes[i];
			boolean checksum = aofilestatus.getChecksum();
			String compressType = aofilestatus.getCompressType();
			int blocksize = aofilestatus.getBlockSize();
			Path path = new Path(pathStr);
			HAWQAOSplit aosplit;
			if (fileLength != 0)
			{
				FileSystem fs = path.getFileSystem(conf);
				BlockLocation[] blkLocations = fs.getFileBlockLocations(
						fs.getFileStatus(path), 0, fileLength);
				// not splitable
				aosplit = new HAWQAOSplit(path, 0, fileLength,
						blkLocations[0].getHosts(), checksum, compressType,
						blocksize);
			}
			else
			{
				// Create empty hosts array for zero length files
				aosplit = new HAWQAOSplit(path, 0, fileLength, new String[0],
						checksum, compressType, blocksize);
			}

			String tableEncoding = metadata.getTableEncoding();
			HAWQSchema schema = metadata.getSchema();
			String version = metadata.getVersion();

			HAWQAOFileReader reader = new HAWQAOFileReader(conf, aosplit);

			HAWQAORecord record = new HAWQAORecord(schema, tableEncoding,
					version);

			long begin = System.currentTimeMillis();
			while (reader.readRecord(record))
			{
				if (whetherToLog)
				{
					int columnCount = record.getSchema().getFieldCount();
					bw.write(record.getString(1));
					for (int j = 2; j <= columnCount; j++)
					{
						bw.write("|");
						bw.write(record.getString(j));
					}
					bw.write("\n");
				}
			}
			long end = System.currentTimeMillis();
			System.out.println("Time elapsed: " + (end - begin) + " for "
					+ fileAttributes[i]);
		}
		if (whetherToLog)
		{
			bw.close();
		}
		*/
	}
}
