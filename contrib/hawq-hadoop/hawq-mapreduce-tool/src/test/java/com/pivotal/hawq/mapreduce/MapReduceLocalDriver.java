package com.pivotal.hawq.mapreduce;

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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * A mapreduce driver to run HAWQInputFormat in local mode. Suitable for Unit Test.
 */
public final class MapReduceLocalDriver extends MapReduceDriver {

	public MapReduceLocalDriver() {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");

		setConf(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2 && args.length != 3) {
			System.err.printf("Usage: %s [generic options] <metadata_file> <output> [<mapper_classname>]\n",
							  getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		String metadataFile = args[0];
		Path outputPath = new Path(args[1]);
		Class<? extends Mapper> mapperClass = (args.length == 2)
								? HAWQTableMapper.class
								: (Class<? extends Mapper>) Class.forName(args[2]);

		// delete previous output
		FileSystem fs = FileSystem.getLocal(getConf());
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		fs.close();

		Job job = new Job(getConf());
		job.setJarByClass(MapReduceLocalDriver.class);

		job.setInputFormatClass(HAWQInputFormat.class);
		HAWQInputFormat.setInput(job.getConfiguration(), metadataFile);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(mapperClass);
		job.setReducerClass(HAWQTableReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
