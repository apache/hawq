package com.pivotal.hawq.mapreduce.parquet;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.parquet.support.HAWQWriteSupport;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.apache.hadoop.mapreduce.Job;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.util.ContextUtil;

class HAWQParquetOutputFormat extends ParquetOutputFormat<HAWQRecord> {

	private static HAWQSchema hawqSchema;

	public HAWQParquetOutputFormat() {
		super(new HAWQWriteSupport());
	}

	public static void setSchema(Job job, HAWQSchema schema) {
		hawqSchema = schema;
		HAWQWriteSupport.setSchema(ContextUtil.getConfiguration(job), hawqSchema);
	}

	public static HAWQRecord newRecord() {
		if (hawqSchema == null) {
			throw new IllegalStateException("you haven't set HAWQSchema yet");
		}
		return new HAWQRecord(hawqSchema);  // TODO reuse record?
	}
}
