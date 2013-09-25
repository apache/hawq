package com.pivotal.hawq.mapreduce.util;

import java.io.IOException;
import java.io.*;
import java.util.Map;

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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.pivotal.hawq.mapreduce.HAWQInputFormat;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.HAWQException;
/**
 * This is common map/reduce program to use HAWQInputFormat.
 */
public class HAWQInputFormatCommon extends Configured {

        private static String HADOOP_HOME;

        public HAWQInputFormatCommon()
        {
            HADOOP_HOME = getHadoopHome();
        }
   
	public static class HAWQMapper extends
			Mapper<Void, HAWQRecord, Text, Text> {
		public void map(Void key, HAWQRecord value, Context context)
				throws IOException, InterruptedException {
			try {
				StringBuffer buffer = new StringBuffer();
				int columnCount = value.getSchema().getFieldCount();
				buffer.append(value.getString(1));
				for (int j = 2; j <= columnCount; j++) {
					buffer.append("|");
					buffer.append(value.getString(j));
				}
				context.write(new Text(buffer.toString()), null);
			} catch (HAWQException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class HAWQMapper_Perf extends
			Mapper<Void, HAWQRecord, Text, Text> {
		public void map(Void key, HAWQRecord value, Context context)
				throws IOException, InterruptedException {
			try {
				int columnCount = value.getSchema().getFieldCount();
				for (int j = 1; j <= columnCount; j++) {
					String str = value.getString(j);
				}
			} catch (HAWQException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class HAWQReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, null);
		}
	}

	private static int printUsage() {
		System.out
				.println("HAWQInputFormatCommon <database_url> <table_name> <output_path> [username] [password]");
		ToolRunner.printGenericCommandUsage(System.out);
		return 2;
	}

	/**
	 * The common method for map/reduce program. Invoke this method to submit the
	 * map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */
	public int runMapReduce(String job_name, String db_url, String table_name, String output_path) throws Exception {
                
                return runMapReduce(job_name, db_url, table_name, output_path, false);
        }       

	/**
	 * The common method for map/reduce program. Invoke this method to submit the
	 * map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */
	public int runMapReduce(String job_name, String db_url, String table_name, String output_path, boolean perf_test) throws Exception {
                
                Configuration conf = new Configuration();
		conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/hdfs-site.xml"));
                conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
		conf.reloadConfiguration();
		Job job = new Job(conf, job_name);

                job.setJarByClass(HAWQInputFormatCommon.class);

 		//For performance test, set the specific Mapper.                
                if(perf_test)
			job.setMapperClass(HAWQMapper_Perf.class);
                else job.setMapperClass(HAWQMapper.class);

		job.setReducerClass(HAWQReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(HAWQInputFormat.class);

		String user_name = null;
		String password = null;

		HAWQInputFormat.setInput(job.getConfiguration(), db_url, user_name,
				password, table_name);
               
                String tempHdfsPath = new String("/temp/result");
                
                //remove the file in hdfs if it exists.
                FileSystem fileSystem = FileSystem.get(conf);
                if(fileSystem.exists(new Path(tempHdfsPath)))
                    fileSystem.delete(new Path(tempHdfsPath));                

		FileOutputFormat.setOutputPath(job, new Path(tempHdfsPath));

		job.setNumReduceTasks(0);
		long beginTime = System.currentTimeMillis();
		int res = job.waitForCompletion(true) ? 0 : 1;
		long endTime = System.currentTimeMillis();
                
                //Dump the file to a local file
                Process process = Runtime.getRuntime().exec("hadoop fs -cat " + tempHdfsPath + "/*");
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(output_path),"UTF-8");
                String s;
           	while( (s = br.readLine())!=null)
                    out.write(s+"\n");
                out.flush();
                out.close();
                process.waitFor();   
                
                //Delete the file in hdfs for next run
                fileSystem.delete(new Path(tempHdfsPath));                
                fileSystem.close();

		System.out.println("Time elapsed:" + (endTime - beginTime));
		return res;
	}

        public void extractMetadata(String dbname, String port, String tablename, String output_path)  throws Exception {
               String cmd = String.format("gpextract -p %s -d %s -o %s %s", port, dbname, output_path, tablename);
               Process process = Runtime.getRuntime().exec(cmd);
               process.waitFor();
               System.out.println("statement execute success :gpextract completed !");
        }
        public int fetchFromMetadata(String job_name, String metadata_path, String output_path) throws Exception {
                
                Configuration conf = new Configuration();
		conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/hdfs-site.xml"));
                conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
		conf.reloadConfiguration();
		Job job = new Job(conf, job_name);

                job.setJarByClass(HAWQInputFormatCommon.class);
		job.setMapperClass(HAWQMapper.class);
		job.setReducerClass(HAWQReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(HAWQInputFormat.class);

		HAWQInputFormat.setInput(job.getConfiguration(), metadata_path);
                
                String tempHdfsPath = new String("/temp/result");
		FileOutputFormat.setOutputPath(job, new Path(tempHdfsPath));

		job.setNumReduceTasks(0);
		long beginTime = System.currentTimeMillis();
		int res = job.waitForCompletion(true) ? 0 : 1;
		long endTime = System.currentTimeMillis();
                
                //Dump the file to a local file
                Process process = Runtime.getRuntime().exec("hadoop fs -cat " + tempHdfsPath + "/*");
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(output_path),"UTF-8");
                String s;
           	while( (s = br.readLine())!=null)
                    out.write(s+"\n");
                out.flush();
                out.close();
                process.waitFor();   
                
                //Delete the file in hdfs for next run
                FileSystem fileSystem = FileSystem.get(conf);
                fileSystem.delete(new Path(tempHdfsPath));                
                fileSystem.close();

		System.out.println("Time elapsed:" + (endTime - beginTime));
		return res;
              }

   	public String getHadoopHome()
    	{
        	String Hadoop_home = "/usr/local/hadoop-2.0.2-alpha-gphd-2.0.1";
       	 	try{
           	 Map m = System.getenv();
            	if(m.containsKey("HADOOP_HOME"))
           	{
                	Hadoop_home = m.get("HADOOP_HOME").toString();
                	return Hadoop_home;
            	}
            	return Hadoop_home;
        	}catch (Exception e) {
            	e.printStackTrace();
           	return "";
       	}
    }

}

