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


import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.schema.HAWQPrimitiveField;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Job for writing HAWQ parquet file
 * User: gaod1
 * Date: 9/16/13
 */
public class HAWQParquetOutputDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HAWQParquetOutputDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "HAWQParquetOutputFormat");
		job.setJarByClass(HAWQParquetOutputDriver.class);

		job.setOutputFormatClass(HAWQParquetOutputFormat.class);

		/*
		// int2 int4 int8
		HAWQSchema schema = new HAWQSchema("t_int",
				HAWQSchema.required_field(HAWQPrimitiveField.PrimitiveType.INT2, "col_short"),
				HAWQSchema.optional_field(HAWQPrimitiveField.PrimitiveType.INT4, "col_int"),
				HAWQSchema.required_field(HAWQPrimitiveField.PrimitiveType.INT8, "col_long")
		);
		job.setMapperClass(WriteIntMapper.class);
		*/

		/*
		// varchar
		HAWQSchema schema = new HAWQSchema("t_varchar",
				HAWQSchema.required_field(HAWQPrimitiveField.PrimitiveType.VARCHAR, "col_varchar")
		);
		job.setMapperClass(WriteVarcharMapper.class);
		*/

		/*
		// float4 float8
		HAWQSchema schema = new HAWQSchema("t_floating",
				HAWQSchema.required_field(HAWQPrimitiveField.PrimitiveType.FLOAT4, "col_float"),
				HAWQSchema.required_field(HAWQPrimitiveField.PrimitiveType.FLOAT8, "col_long")
		);
		job.setMapperClass(WriteFloatingNumberMapper.class);
		*/

		// boolean
//		HAWQSchema schema = new HAWQSchema("t_boolean",
//				HAWQSchema.required_field(HAWQPrimitiveField.PrimitiveType.BOOL, "col_bool"));
//		job.setMapperClass(WriteBooleanMapper.class);

		// byte array
		HAWQSchema schema = new HAWQSchema("t_bytea",
				HAWQSchema.required_field(HAWQPrimitiveField.PrimitiveType.BYTEA, "col_bytea"));
		job.setMapperClass(WriteByteArrayMapper.class);

		HAWQParquetOutputFormat.setSchema(job, schema);


		FileInputFormat.addInputPath(job, new Path(args[0]));
		HAWQParquetOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(Void.class);
		job.setMapOutputValueClass(HAWQRecord.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static class WriteIntMapper extends Mapper<LongWritable, Text, Void, HAWQRecord> {

		private HAWQRecord record = HAWQParquetOutputFormat.newRecord();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Integer recordNum = Integer.parseInt(value.toString());
			try {
				for (int i = 0; i < recordNum; i++) {
					record.reset();
					record.setShort(1, (short) (i + 1));
					if (i % 2 == 0) {
						record.setInt(2, i);
					}
					record.setLong(3, i * 100);
					context.write(null, record);
				}

			} catch (HAWQException e) {
				throw new IOException(e);
			}
		}
	}

	private static class WriteVarcharMapper extends Mapper<LongWritable, Text, Void, HAWQRecord> {
		private HAWQRecord record = HAWQParquetOutputFormat.newRecord();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Integer recordNum = Integer.parseInt(value.toString());
			try {
				for (int i = 0; i < recordNum; i++) {
					record.reset();
					record.setString(1, "hello" + i);
					context.write(null, record);
				}

			} catch (HAWQException e) {
				throw new IOException(e);
			}
		}
	}

	private static class WriteFloatingNumberMapper extends Mapper<LongWritable, Text, Void, HAWQRecord> {
		private HAWQRecord record = HAWQParquetOutputFormat.newRecord();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Integer recordNum = Integer.parseInt(value.toString());
			try {
				for (int i = 0; i < recordNum; i++) {
					record.reset();
					record.setFloat(1, 1.0f * i);
					record.setDouble(2, 2 * Math.PI * i);
					context.write(null, record);
				}

			} catch (HAWQException e) {
				throw new IOException(e);
			}
		}
	}

	private static class WriteBooleanMapper extends Mapper<LongWritable, Text, Void, HAWQRecord> {
		private HAWQRecord record = HAWQParquetOutputFormat.newRecord();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Integer recordNum = Integer.parseInt(value.toString());
			try {
				for (int i = 0; i < recordNum; i++) {
					record.reset();
					record.setBoolean(1, i % 2 == 0);
					context.write(null, record);
				}

			} catch (HAWQException e) {
				throw new IOException(e);
			}
		}
	}

	private static class WriteByteArrayMapper extends Mapper<LongWritable, Text, Void, HAWQRecord> {
		private HAWQRecord record = HAWQParquetOutputFormat.newRecord();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Integer recordNum = Integer.parseInt(value.toString());
			try {
				for (int i = 0; i < recordNum; i++) {
					record.reset();
					record.setBytes(1, String.format("hello %d", i).getBytes());
					context.write(null, record);
				}

			} catch (HAWQException e) {
				throw new IOException(e);
			}
		}
	}
}
