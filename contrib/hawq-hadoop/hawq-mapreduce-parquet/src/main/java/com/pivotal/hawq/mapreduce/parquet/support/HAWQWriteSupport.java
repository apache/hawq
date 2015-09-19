package com.pivotal.hawq.mapreduce.parquet.support;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.HAWQRecord;
import com.pivotal.hawq.mapreduce.parquet.convert.HAWQRecordWriter;
import com.pivotal.hawq.mapreduce.parquet.util.HAWQSchemaConverter;
import com.pivotal.hawq.mapreduce.schema.HAWQSchema;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;

import java.util.HashMap;
import java.util.Map;

/**
 * User: gaod1
 * Date: 9/10/13
 */
public class HAWQWriteSupport extends WriteSupport<HAWQRecord> {

	// key of HAWQ Schema in extraKeyValue of HAWQ's parquet file
	private static final String HAWQ_SCHEMA_KEY = "hawq.schema";

	private HAWQSchema hawqSchema;
	private MessageType parquetSchema;
	private HAWQRecordWriter recordWriter;

	public static void setSchema(Configuration configuration, HAWQSchema hawqSchema) {
		configuration.set("parquet.hawq.schema", hawqSchema.toString());
	}

	@Override
	public WriteContext init(Configuration configuration) {
		hawqSchema = HAWQSchema.fromString(configuration.get("parquet.hawq.schema"));
		parquetSchema = HAWQSchemaConverter.convertToParquet(hawqSchema);

		Map<String, String> extraMetaData = new HashMap<String, String>();
		extraMetaData.put(HAWQ_SCHEMA_KEY, hawqSchema.toString());

		return new WriteContext(parquetSchema, extraMetaData);
	}

	@Override
	public void prepareForWrite(RecordConsumer recordConsumer) {
		recordWriter = new HAWQRecordWriter(recordConsumer, hawqSchema);
	}

	@Override
	public void write(HAWQRecord record) {
		try {
			recordWriter.writeRecord(record);
		} catch (HAWQException e) {
			throw new RuntimeException("failed to write record", e);
		}
	}
}
