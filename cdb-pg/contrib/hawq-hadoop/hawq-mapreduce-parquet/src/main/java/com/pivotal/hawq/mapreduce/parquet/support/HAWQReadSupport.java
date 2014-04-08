package com.pivotal.hawq.mapreduce.parquet.support;

import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.parquet.convert.HAWQRecordMaterializer;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import java.util.Map;

/**
 * User: gaod1
 * Date: 8/8/13
 */
public class HAWQReadSupport extends ReadSupport<HAWQRecord> {

	private static final String KEY_HAWQ_SCHEMA = "hawq.schema";
	private static final String HAWQ_REQUESTED_SCHEMA = "hawq.schema.requested";

	@Override
	public ReadContext init(Configuration configuration,
							Map<String, String> keyValueMetaData,
							MessageType fileSchema) {

//		String requestedProjectionString = configuration.get(HAWQ_REQUESTED_SCHEMA);
//
//		if (requestedProjectionString == null) { // read all data
//			return new ReadContext(fileSchema);
//		}
//
//		HAWQSchema requestedHAWQSchema = HAWQSchema.fromString(requestedProjectionString);
//		MessageType requestedSchema = HAWQSchemaConverter.convertToParquet(requestedHAWQSchema);
//		return new ReadContext(requestedSchema);

		return new ReadContext(fileSchema);
	}

	@Override
	public RecordMaterializer<HAWQRecord> prepareForRead(Configuration configuration,
														 Map<String, String> keyValueMetaData,
														 MessageType fileSchema, ReadContext readContext) {

		HAWQSchema hawqSchema = HAWQSchema.fromString(keyValueMetaData.get(KEY_HAWQ_SCHEMA));
		return new HAWQRecordMaterializer(
				readContext.getRequestedSchema(), // requested parquet schema
				hawqSchema); // corresponding requested HAWQSchema
	}
}
