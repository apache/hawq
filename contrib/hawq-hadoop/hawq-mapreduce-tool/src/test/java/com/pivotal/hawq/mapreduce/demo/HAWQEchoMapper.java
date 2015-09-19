package com.pivotal.hawq.mapreduce.demo;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * A mapper which echos each field of {@link com.pivotal.hawq.mapreduce.HAWQRecord} separated by ','.
 */
public class HAWQEchoMapper extends Mapper<Void, HAWQRecord, Text, Text> {
	@Override
	protected void map(Void key, HAWQRecord value, Context context) throws IOException, InterruptedException {
		try {
			StringBuffer buf = new StringBuffer(value.getString(1));
			for (int i = 2; i <= value.getSchema().getFieldCount(); i++) {
				buf.append(",").append(value.getString(i));
			}
			context.write(new Text(buf.toString()), null);

		} catch (HAWQException e) {
			throw new IOException(e);
		}
	}
}
