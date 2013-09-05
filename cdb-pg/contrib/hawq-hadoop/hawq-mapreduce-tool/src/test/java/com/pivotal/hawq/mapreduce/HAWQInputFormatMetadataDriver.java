package com.pivotal.hawq.mapreduce;

import com.pivotal.hawq.mapreduce.HAWQException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*
 * This is a demo for HAWQInputFormat with Metadata Interface
 */
public class HAWQInputFormatMetadataDriver extends Configured implements Tool
{

	public static class HAWQMapper extends Mapper<Void, HAWQRecord, Text, Text>
	{
		public void map(Void key, HAWQRecord value, Context context)
				throws IOException, InterruptedException
		{

			try
			{
				StringBuffer buffer = new StringBuffer();
				int columnCount = value.getSchema().getFieldCount();
				buffer.append(value.getString(1));
				for (int j = 2; j <= columnCount; j++)
				{
					buffer.append("|");
					buffer.append(value.getString(j));
				}
				context.write(new Text(buffer.toString()), null);

			}
			catch (HAWQException e)
			{
				throw new IOException(e);
			}
		}
	}

	public static class HAWQReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException
		{
			context.write(key, null);
		}
	}

	private static int printUsage()
	{
		System.out
				.println("HAWQInputFormatMetadataDriver <metadata_pathStr> <output_path>");
		ToolRunner.printGenericCommandUsage(System.out);
		return 2;
	}

	public static void main(String[] args) throws Exception
	{

		int res = ToolRunner.run(new Configuration(),
				new HAWQInputFormatMetadataDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception
	{

		if (args.length < 2)
		{
			return printUsage();
		}

		Job job = new Job(getConf());
		// Job job = Job.getInstance(getConf());
		job.setJobName("hawqinputformat");
		job.setJarByClass(HAWQInputFormatDriver.class);

		job.setMapperClass(HAWQMapper.class);
		job.setReducerClass(HAWQReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		String metadata_path = args[0];
		String output_path = args[1];

		job.setInputFormatClass(HAWQInputFormat.class);
		HAWQInputFormat.setInput(job.getConfiguration(), metadata_path);

		FileOutputFormat.setOutputPath(job, new Path(output_path));

		job.setNumReduceTasks(0);
		long beginTime = System.currentTimeMillis();
		int res = job.waitForCompletion(true) ? 0 : 1;
		long endTime = System.currentTimeMillis();
		System.out.println("Time elapsed:" + (endTime - beginTime));
		return res;
	}

}