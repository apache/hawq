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

import java.util.Map;

/**
 * A mapreduce driver to run HAWQInputFormat in cluster mode. Suitable for Integration Test.
 */
public final class MapReduceClusterDriver extends MapReduceDriver {

	private static final String DEFAULT_HADOOP_HOME = "/usr/local/hadoop";
	private static final String HADOOP_HOME;
	static {
		Map<String, String> env = System.getenv();
		if (env.containsKey("HADOOP_HOME"))
			HADOOP_HOME = env.get("HADOOP_HOME");
		else
			HADOOP_HOME = DEFAULT_HADOOP_HOME;
	}

	public MapReduceClusterDriver() {
		Configuration conf = new Configuration();
		conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));

		setConf(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3 && args.length != 4) {
			System.err.printf("Usage: %s [generic options] <tableName> <dburl> <output> [<mapper_classname>]\n",
							  getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		String tableName	= args[0];
		String dbUrl		= args[1];
		Path outputPath		= new Path(args[2]);
		Class<? extends Mapper> mapperClass = (args.length == 3)
				? HAWQTableMapper.class
				: (Class<? extends Mapper>) Class.forName(args[3]);

		// delete previous output
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		fs.close();

		Job job = new Job(getConf(), "job_read_" + tableName);
		job.setJarByClass(MapReduceClusterDriver.class);

		job.setInputFormatClass(HAWQInputFormat.class);
		HAWQInputFormat.setInput(job.getConfiguration(), dbUrl, null, null, tableName);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(mapperClass);
		job.setReducerClass(HAWQTableReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
