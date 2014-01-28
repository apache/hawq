package com.pivotal.pxf.plugins.hdfs;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import com.pivotal.pxf.plugins.hdfs.utilities.RecordkeyAdapter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static com.pivotal.pxf.api.io.DataType.*;


/*
 * Class AvroResolver handles deserialization of records that were serialized 
 * using the AVRO serialization framework. AvroResolver implements
 * IReadResolver interface.
 */
public class AvroResolver extends Plugin implements ReadResolver {
    private GenericRecord avroRecord = null;
    private DatumReader<GenericRecord> reader = null;
    private BinaryDecoder decoder = null;  // member kept to enable reuse, and thus avoid repeated allocation
    private List<Schema.Field> fields = null;
    private RecordkeyAdapter recordkeyAdapter = new RecordkeyAdapter();

    /*
     * C'tor
     * Initializes Avro data structure: the avro record - fields information and the avro record reader
     * All Avro data is build from the Avro schema, which is based on the *.avsc file that was passed
     * by the user
     */
    public AvroResolver(InputData input) throws IOException {
        super(input);

        Schema schema = isAvroFile()
                ? (Schema) inputData.getSchema()
                : (new Schema.Parser()).parse(openExternalSchema());

        reader = new GenericDatumReader<GenericRecord>(schema);
        fields = schema.getFields();
    }

    /*
     * getFields returns a list of the fields of one record.
     * Each record field is represented by a OneField item.
     * OneField item contains two fields: an integer representing the field type and a Java
     * Object representing the field value.
     */
    public List<OneField> getFields(OneRow row) throws Exception {
        avroRecord = makeAvroRecord(row.getData(), avroRecord);
        List<OneField> record = new LinkedList<OneField>();

        int recordkeyIndex = (inputData.getRecordkeyColumn() == null) ? -1 :
                inputData.getRecordkeyColumn().columnIndex();
        int currentIndex = 0;

        for (Schema.Field field : fields) {
            /*
             * Add the record key if exists
			 */
            if (currentIndex == recordkeyIndex) {
                currentIndex += recordkeyAdapter.appendRecordkeyField(record, inputData, row);
            }

            currentIndex += populateRecord(record, field);
        }

        return record;
    }

    /*
     * Test if the Avro records are residing inside an AVRO file.
     * If the Avro records are not residing inside an AVRO file, then
     * they may reside inside a sequence file, regular file, ...
     */
    boolean isAvroFile() {
        return inputData.accessor().toLowerCase().contains("avro");
    }

    /*
     * The record can arrive from one out of two different sources: a sequence file or an AVRO file.
     * If it comes from an AVRO file, then it was already obtained as a GenericRecord when
     * when it was fetched from the file with the AvroRecorReader so in this case a cast is enough.
     * On the other hand, if the source is a sequence file, then the input parameter
     * obj hides a bytes [] buffer which is in fact one Avro record serialized.
     * Here, we build the Avro record from the flat buffer, using the AVRO API.
     * Then (for both cases) in the remaining functions we build a List<OneField> record from
     * the Avro record
     */
    GenericRecord makeAvroRecord(Object obj, GenericRecord reuseRecord) throws IOException {
        if (isAvroFile()) {
            return (GenericRecord) obj;
        } else {
            byte[] bytes = ((BytesWritable) obj).getBytes();
            decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
            return reader.read(reuseRecord, decoder);
        }
    }

    /*
     * For a given field in the Avro record we extract its value and insert it into the output
     * List<OneField> record. An Avro field can be a primitive type or an array type.
     */
    int populateRecord(List<OneField> record, Schema.Field field) throws IllegalAccessException {
        String fieldName = field.name();
        Schema fieldSchema = field.schema();
        Schema.Type fieldType = fieldSchema.getType();
        int ret = 0;

        switch (fieldType) {
            case ARRAY:
                ret = SetArrayField(record, fieldName, fieldSchema);
                break;
            case INT:
                ret = addOneFieldToRecord(record, INTEGER, avroRecord.get(fieldName));
                break;
            case DOUBLE:
                ret = addOneFieldToRecord(record, FLOAT8, avroRecord.get(fieldName));
                break;
            case STRING:
                ret = addOneFieldToRecord(record, VARCHAR, avroRecord.get(fieldName));
                break;
            case FLOAT:
                ret = addOneFieldToRecord(record, REAL, avroRecord.get(fieldName));
                break;
            case LONG:
                ret = addOneFieldToRecord(record, BIGINT, avroRecord.get(fieldName));
                break;
            case BYTES:
                ret = addOneFieldToRecord(record, BYTEA, avroRecord.get(fieldName));
                break;
            case BOOLEAN:
                ret = addOneFieldToRecord(record, BOOLEAN, avroRecord.get(fieldName));
                break;
            default:
                break;
        }
        return ret;
    }

    /*
     * When an Avro field is actually an array, we resolve the type of the array element, and for
     * each element in the Avro array, we create an object of type OneField and insert it into the
     * output List<OneField> record
     */
    int SetArrayField(List<OneField> record, String fieldName, Schema arraySchema) throws IllegalAccessException {
        Schema typeSchema = arraySchema.getElementType();
        Schema.Type arrayType = typeSchema.getType();
        int ret = 0;

        switch (arrayType) {
            case INT:
                ret = iterateArray(record, INTEGER, (GenericData.Array<?>) avroRecord.get(fieldName));
                break;
            case DOUBLE:
                ret = iterateArray(record, FLOAT8, (GenericData.Array<?>) avroRecord.get(fieldName));
                break;
            case STRING:
                ret = iterateArray(record, VARCHAR, (GenericData.Array<?>) avroRecord.get(fieldName));
                break;
            case FLOAT:
                ret = iterateArray(record, REAL, (GenericData.Array<?>) avroRecord.get(fieldName));
                break;
            case LONG:
                ret = iterateArray(record, BIGINT, (GenericData.Array<?>) avroRecord.get(fieldName));
                break;
            case BYTES:
                ret = iterateArray(record, BYTEA, (GenericData.Array<?>) avroRecord.get(fieldName));
                break;
            case BOOLEAN:
                ret = iterateArray(record, BOOLEAN, (GenericData.Array<?>) avroRecord.get(fieldName));
                break;
            default:
                break;
        }
        return ret;
    }

    /*
     * Iterate the Avro array (that comes from an Avro field) and for each array element create  a OneField
     * object and add it to the output List<OneField> record.
     */
    int iterateArray(List<OneField> record, DataType gpdbWritableType, GenericData.Array<?> array) {
        int length = array.size();
        for (int i = 0; i < length; i++) {
            addOneFieldToRecord(record, gpdbWritableType, array.get(i));
        }
        return length;
    }

    /*
     * Creates the OneField object and adds it to the output List<OneField> record.
     * Strings and byte arrays are held inside special types in the Avro record so we
     * transfer them to standard types in order to enable their insertion in the
     * GPDBWritable instance
     */
    int addOneFieldToRecord(List<OneField> record, DataType gpdbWritableType, Object val) {
        OneField oneField = new OneField();
        oneField.type = gpdbWritableType.getOID();
        switch (gpdbWritableType) {
            case VARCHAR:
                oneField.val = ((Utf8) val).toString();
                break;
            case BYTEA:
                oneField.val = ((ByteBuffer) val).array();
                break;
            default:
                oneField.val = val;
                break;
        }

        record.add(oneField);
        return 1;
    }

    InputStream openExternalSchema() throws IOException {
        String schemaName = inputData.srlzSchemaName();
        ClassLoader loader = this.getClass().getClassLoader();
        InputStream result = loader.getResourceAsStream(schemaName);

        return result;
    }
}
