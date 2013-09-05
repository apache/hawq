package com.pivotal.hawq.mapreduce.parquet.convert;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.parquet.HAWQParquetRecord;
import com.pivotal.hawq.mapreduce.parquet.convert.HAWQRecordConverter;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

/**
 * User: gaod1
 * Date: 8/8/13
 */
public class HAWQRecordMaterializer extends RecordMaterializer<HAWQRecord> {

	private HAWQRecordConverter rootConverter;

	public HAWQRecordMaterializer(MessageType requestedSchema, HAWQSchema hawqSchema) {
		rootConverter = new HAWQRecordConverter(requestedSchema, hawqSchema);
	}

	@Override
	public HAWQParquetRecord getCurrentRecord() {
		return rootConverter.getCurrentRecord();
	}

	@Override
	public GroupConverter getRootConverter() {
		return rootConverter;
	}
}
