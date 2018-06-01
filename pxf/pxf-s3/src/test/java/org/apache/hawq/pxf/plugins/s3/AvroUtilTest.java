package org.apache.hawq.pxf.plugins.s3;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.s3.AvroUtil;
import org.junit.Test;

public class AvroUtilTest {

	@Test
	public void testSchemaFromInputData() {
		InputData id = new TestInputData();
		ArrayList<ColumnDescriptor> tupleDescription = new ArrayList<>();
		tupleDescription.add(new ColumnDescriptor("int_col", DataType.INTEGER.getOID(), 0, "int4", null));
		tupleDescription.add(new ColumnDescriptor("text_col", DataType.TEXT.getOID(), 1, "text", null));
		tupleDescription.add(new ColumnDescriptor("bool_col", DataType.BOOLEAN.getOID(), 2, "bool", null));
		((TestInputData) id).setTupleDescription(tupleDescription);
		Schema schemaFromInputData = AvroUtil.schemaFromInputData(id);
		assertNotNull(schemaFromInputData);
		Schema schemaFromAPI = SchemaBuilder.record("mytable").namespace(AvroUtil.NAMESPACE)
				   .fields()
				   .name("int_col").type().nullable().intType().noDefault()
				   .name("text_col").type().nullable().stringType().noDefault()
				   .name("bool_col").type().nullable().booleanType().noDefault()
				   .endRecord();
		assertEquals(schemaFromInputData, schemaFromAPI);
	}
	
	@Test
	public void testAddQuotes() {
		String withoutQuotes = "this has no quotes yet";
		String withQuotes = "\"this has no quotes yet\"";
		assertEquals(withQuotes, AvroUtil.addQuotes(withoutQuotes));
	}
	
	@Test
	public void testAsAvroType() {
		assertEquals("\"boolean\"", AvroUtil.asAvroType(DataType.BOOLEAN));
		assertEquals("\"bytes\"", AvroUtil.asAvroType(DataType.BYTEA));
		assertEquals("\"long\"", AvroUtil.asAvroType(DataType.BIGINT));
		assertEquals("\"int\"", AvroUtil.asAvroType(DataType.INTEGER));
		assertEquals("\"string\"", AvroUtil.asAvroType(DataType.TEXT));
		assertEquals("\"float\"", AvroUtil.asAvroType(DataType.REAL));
		assertEquals("\"double\"", AvroUtil.asAvroType(DataType.FLOAT8));
		assertEquals("{ \"type\": \"string\", \"logicalType\": \"date\" }", AvroUtil.asAvroType(DataType.DATE));
		assertEquals("{ \"type\": \"string\", \"logicalType\": \"timestamp-millis\" }", AvroUtil.asAvroType(DataType.TIMESTAMP));
	}

	/**
	 * This exists solely to permit setting the tupleDescription field of the
	 * InputData instance
	 */
	static class TestInputData extends InputData {
		public TestInputData() {
			super();
		}

		public void setTupleDescription(ArrayList<ColumnDescriptor> tupleDescription) {
			super.tupleDescription = tupleDescription;
		}
	}
}
