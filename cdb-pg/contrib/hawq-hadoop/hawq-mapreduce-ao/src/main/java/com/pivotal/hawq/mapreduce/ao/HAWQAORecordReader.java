package com.pivotal.hawq.mapreduce.ao;

import com.pivotal.hawq.mapreduce.conf.HAWQConfiguration;
import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.ao.io.HAWQAOFileReader;
import com.pivotal.hawq.mapreduce.ao.io.HAWQAORecord;

import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class HAWQAORecordReader extends RecordReader<Void, HAWQRecord> {

	private HAWQRecord value = null;
	private HAWQAOFileReader filereader = null;
	private boolean more = true;

	@Override
	public void close() throws IOException {
		filereader.close();
	}

	@Override
	public Void getCurrentKey() throws IOException, InterruptedException {
		// Always null
		return null;
	}

	@Override
	public HAWQRecord getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return more ? 0f : 100f;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		// initialize the value
		Configuration conf = context.getConfiguration();

		// Extract the parameters needed by HAWQAOFileReader and HAWQAORecord
		String encoding = HAWQConfiguration.getInputTableEncoding(conf);
        HAWQSchema schema = HAWQConfiguration.getInputTableSchema(conf);

		filereader = new HAWQAOFileReader(conf, split);

		try {
			value = new HAWQAORecord(schema, encoding);
		} catch (HAWQException hawqE) {
			throw new IOException(hawqE.getMessage());
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		try {
			if (filereader.readRecord((HAWQAORecord)value)) {
				return true;
			}
		} catch (HAWQException hawqE) {
			throw new IOException(hawqE.getMessage());
		}
		more = false;
		return false;
	}
}