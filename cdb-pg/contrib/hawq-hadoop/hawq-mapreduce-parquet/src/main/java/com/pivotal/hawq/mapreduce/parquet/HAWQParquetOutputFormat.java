package com.pivotal.hawq.mapreduce.parquet;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.pivotal.hawq.mapreduce.HAWQRecord;

public class HAWQParquetOutputFormat extends FileOutputFormat<Void, HAWQRecord> {

	@Override
	public RecordWriter<Void, HAWQRecord> getRecordWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}
