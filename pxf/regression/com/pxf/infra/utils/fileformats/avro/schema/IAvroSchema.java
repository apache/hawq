package com.pxf.infra.utils.fileformats.avro.schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

public interface IAvroSchema {

	public GenericRecord serialize() throws IOException;

	public void serialize(ByteArrayOutputStream stream) throws IOException;
}
