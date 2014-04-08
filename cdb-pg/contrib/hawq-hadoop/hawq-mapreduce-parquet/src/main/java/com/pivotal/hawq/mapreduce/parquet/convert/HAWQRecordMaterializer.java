package com.pivotal.hawq.mapreduce.parquet.convert;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import parquet.io.api.GroupConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

/**
 * HAWQ's implementation of the RecordMaterializer interface.
 *
 * <p>This class materialize HAWQRecord objects from a stream of Parquet data,
 * using a HAWQRecordConverter internally to convert data types between HAWQ and Parquet.
 *
 */
public class HAWQRecordMaterializer extends RecordMaterializer<HAWQRecord> {

	private HAWQRecordConverter rootConverter;

	public HAWQRecordMaterializer(MessageType requestedSchema, HAWQSchema hawqSchema) {
		rootConverter = new HAWQRecordConverter(requestedSchema, hawqSchema);
	}

	@Override
	public HAWQRecord getCurrentRecord() {
		return rootConverter.getCurrentRecord();
	}

	@Override
	public GroupConverter getRootConverter() {
		return rootConverter;
	}
}
