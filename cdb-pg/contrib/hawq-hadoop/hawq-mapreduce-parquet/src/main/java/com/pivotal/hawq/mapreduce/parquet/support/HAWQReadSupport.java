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

	@Override
	public ReadContext init(Configuration configuration,
							Map<String, String> keyValueMetaData,
							MessageType fileSchema) {

//		String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA); // TODO use another key for requested schema?
//		MessageType requestedSchema = (partialSchemaString == null) ? fileSchema : getSchemaForRead(fileSchema, partialSchemaString);
//		// TODO client gives requestedHAWQSchema and we construct corresponding requestedMessageType
//		return new ReadContext(requestedSchema);
		return new ReadContext(fileSchema);
	}

	@Override
	public RecordMaterializer<HAWQRecord> prepareForRead(Configuration configuration,
														 Map<String, String> keyValueMetaData,
														 MessageType fileSchema, ReadContext readContext) {

		String hawqSchemaStr = configuration.get("hawq.record.schema", "record t1 { int4 user_id }");
		HAWQSchema hawqSchema = HAWQSchema.fromString(hawqSchemaStr);
		return new HAWQRecordMaterializer(readContext.getRequestedSchema(), hawqSchema);
	}
}
