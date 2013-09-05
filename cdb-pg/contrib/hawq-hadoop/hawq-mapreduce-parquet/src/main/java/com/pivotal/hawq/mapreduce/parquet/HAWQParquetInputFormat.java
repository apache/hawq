package com.pivotal.hawq.mapreduce.parquet;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.parquet.support.HAWQReadSupport;
import parquet.hadoop.ParquetInputFormat;

/**
 * User: gaod1
 * Date: 8/8/13
 */
public class HAWQParquetInputFormat
	extends ParquetInputFormat<HAWQRecord> {

	public HAWQParquetInputFormat() {
		super(HAWQReadSupport.class);
	}

}
