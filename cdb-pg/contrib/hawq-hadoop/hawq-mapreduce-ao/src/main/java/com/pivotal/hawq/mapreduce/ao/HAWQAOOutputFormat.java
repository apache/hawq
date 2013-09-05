package com.pivotal.hawq.mapreduce.ao;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.pivotal.hawq.mapreduce.HAWQRecord;

public class HAWQAOOutputFormat extends FileOutputFormat<Void, HAWQRecord> {

	@Override
	public RecordWriter<Void, HAWQRecord> getRecordWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		return null;
	}

}
