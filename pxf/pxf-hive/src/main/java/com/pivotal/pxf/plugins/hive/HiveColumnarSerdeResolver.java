package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.*;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.ColumnDescriptor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static com.pivotal.pxf.api.io.DataType.VARCHAR;

/**
 * Specialized HiveResolver for a Hive table stored as RC file.
 * Use together with HiveInputFormatFragmenter/HiveRCFileAccessor.
 */
public class HiveColumnarSerdeResolver extends Plugin implements ReadResolver {
    private ColumnarSerDeBase deserializer;
    private String partitionKeys;
    private char delimiter;
    private boolean firstColumn;
    private StringBuilder builder;
    private StringBuilder parts;
    private int numberOfPartitions;
    private static Log Log = LogFactory.getLog(HiveColumnarSerdeResolver.class);
    private HiveInputFormatFragmenter.PXF_HIVE_SERDES serdeType;

    public HiveColumnarSerdeResolver(InputData input) throws Exception {
        super(input);

        numberOfPartitions = 0;
        initUserData(input);
        parseDelimiterChar(input);
        parts = new StringBuilder();
        initPartitionFields();
        initSerde(input);
    }

    /* read the data supplied by the fragmenter: inputformat name, serde name, partition keys */
    private void initUserData(InputData input) throws Exception {
        String[] toks = HiveInputFormatFragmenter.parseToks(input);
        String serdeEnumStr = toks[HiveInputFormatFragmenter.TOK_SERDE];
        if (serdeEnumStr.equals(HiveInputFormatFragmenter.PXF_HIVE_SERDES.COLUMNAR_SERDE.name())) {
            serdeType = HiveInputFormatFragmenter.PXF_HIVE_SERDES.COLUMNAR_SERDE;
        } else if (serdeEnumStr.equals(HiveInputFormatFragmenter.PXF_HIVE_SERDES.LAZY_BINARY_COLUMNAR_SERDE.name())) {
            serdeType = HiveInputFormatFragmenter.PXF_HIVE_SERDES.LAZY_BINARY_COLUMNAR_SERDE;
        } else {
            throw new UnsupportedTypeException("Unsupported Hive Serde: " + serdeEnumStr);
        }

        partitionKeys = toks[HiveInputFormatFragmenter.TOK_KEYS];
    }

    /**
     * getFields returns a singleton list of OneField item.
     * OneField item contains two fields: an integer representing the VARCHAR type and a Java
     * Object representing the field value.
     */
    @Override
    public List<OneField> getFields(OneRow onerow) throws Exception {
        firstColumn = true;
        builder = new StringBuilder();
        Object tuple = deserializer.deserialize((Writable) onerow.getData());
        ObjectInspector oi = deserializer.getObjectInspector();

        traverseTuple(tuple, oi);
        /* We follow Hive convention. Partition fields are always added at the end of the record */
        builder.append(parts);
        return Collections.singletonList(new OneField(VARCHAR.getOID(), builder.toString()));
    }

    /* Get and init the deserializer for the records of this Hive data fragment */
    private void initSerde(InputData input) throws Exception {
        Properties serdeProperties = new Properties();
        int numberOfDataColumns = input.getColumns() - numberOfPartitions;

        Log.debug("Serde number of columns is " + numberOfDataColumns);

        StringBuilder columnNames = new StringBuilder(numberOfDataColumns * 2); // column
        StringBuilder columnTypes = new StringBuilder(numberOfDataColumns * 2);
        String delim = "";
        for (int i = 0; i < numberOfDataColumns; i++) {
            ColumnDescriptor column = input.getColumn(i);
            String columnName = column.columnName();
            String columnType = HiveInputFormatFragmenter.toHiveType(DataType.get(column.columnTypeCode()), columnName);
            columnNames.append(delim).append(columnName);
            columnTypes.append(delim).append(columnType);
            delim = ",";
        }
        serdeProperties.put(serdeConstants.LIST_COLUMNS, columnNames.toString());
        serdeProperties.put(serdeConstants.LIST_COLUMN_TYPES, columnTypes.toString());

        if (serdeType == HiveInputFormatFragmenter.PXF_HIVE_SERDES.COLUMNAR_SERDE) {
            deserializer = new ColumnarSerDe();
        } else if (serdeType == HiveInputFormatFragmenter.PXF_HIVE_SERDES.LAZY_BINARY_COLUMNAR_SERDE) {
            deserializer = new LazyBinaryColumnarSerDe();
        } else {
            throw new UnsupportedTypeException("Unsupported Hive Serde: " + serdeType.name()); /* we should not get here */
        }

        deserializer.initialize(new JobConf(new Configuration(), HiveColumnarSerdeResolver.class), serdeProperties);
    }

    /* The partition fields are initialized  one time base on userData provided by the fragmenter */
    private void initPartitionFields() {
        if (partitionKeys.equals(HiveDataFragmenter.HIVE_NO_PART_TBL)) {
            return;
        }
        String[] partitionLevels = partitionKeys.split(HiveDataFragmenter.HIVE_PARTITIONS_DELIM);
        numberOfPartitions = partitionLevels.length;
        for (String partLevel : partitionLevels) {
            String[] levelKey = partLevel.split(HiveDataFragmenter.HIVE_1_PART_DELIM);
            String type = levelKey[1];
            String val = levelKey[2];
            parts.append(delimiter);
            switch (type) {
                case serdeConstants.STRING_TYPE_NAME:
                    parts.append(val);
                    break;
                case serdeConstants.SMALLINT_TYPE_NAME:
                    parts.append(Short.parseShort(val));
                    break;
                case serdeConstants.INT_TYPE_NAME:
                    parts.append(Integer.parseInt(val));
                    break;
                case serdeConstants.BIGINT_TYPE_NAME:
                    parts.append(Long.parseLong(val));
                    break;
                case serdeConstants.FLOAT_TYPE_NAME:
                    parts.append(Float.parseFloat(val));
                    break;
                case serdeConstants.DOUBLE_TYPE_NAME:
                    parts.append(Double.parseDouble(val));
                    break;
                case serdeConstants.TIMESTAMP_TYPE_NAME:
                    parts.append(Timestamp.valueOf(val));
                    break;
                case serdeConstants.DECIMAL_TYPE_NAME:
                    parts.append(HiveDecimal.create(val).bigDecimalValue());
                    break;
                default:
                    throw new UnsupportedTypeException("Unsupported partition type: " + type);
            }
        }
    }

    /**
     * Handle a Hive record.
     * Supported object categories:
     * Primitive - including NULL
     * Struct (used by ColumnarSerDe to store primitives) - cannot be NULL
     * <p/>
     * Any other category will throw UnsupportedTypeException
     */
    public void traverseTuple(Object obj, ObjectInspector objInspector) throws IOException, BadRecordException {
        ObjectInspector.Category category = objInspector.getCategory();
        if ((obj == null) && (category != ObjectInspector.Category.PRIMITIVE)) {
            throw new BadRecordException("NULL Hive composite object");
        }
        switch (category) {
            case PRIMITIVE:
                resolvePrimitive(obj, (PrimitiveObjectInspector) objInspector);
                break;
            case STRUCT:
                StructObjectInspector soi = (StructObjectInspector) objInspector;
                List<? extends StructField> fields = soi.getAllStructFieldRefs();
                List<?> list = soi.getStructFieldsDataAsList(obj);
                if (list == null) {
                    throw new BadRecordException("Illegal value NULL for Hive data type Struct");
                }
                for (int i = 0; i < list.size(); i++) {
                    traverseTuple(list.get(i), fields.get(i).getFieldObjectInspector());
                }
                break;
            default:
                throw new UnsupportedTypeException("Hive object category: " + objInspector.getCategory() + " unsupported");
        }
    }

    public void resolvePrimitive(Object o, PrimitiveObjectInspector oi) throws IOException {

        if (!firstColumn) {
            builder.append(delimiter);
        }

        if (o == null) {
            builder.append("\\N");
        } else {
            switch (oi.getPrimitiveCategory()) {
                case BOOLEAN:
                    builder.append(((BooleanObjectInspector) oi).get(o));
                    break;
                case SHORT:
                    builder.append(((ShortObjectInspector) oi).get(o));
                    break;
                case INT:
                    builder.append(((IntObjectInspector) oi).get(o));
                    break;
                case LONG:
                    builder.append(((LongObjectInspector) oi).get(o));
                    break;
                case FLOAT:
                    builder.append(((FloatObjectInspector) oi).get(o));
                    break;
                case DOUBLE:
                    builder.append(((DoubleObjectInspector) oi).get(o));
                    break;
                case DECIMAL:
                    builder.append(((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o).bigDecimalValue());
                    break;
                case STRING:
                    builder.append(((StringObjectInspector) oi).getPrimitiveJavaObject(o));
                    break;
                case BINARY:
                    byte[] bytes = ((BinaryObjectInspector) oi).getPrimitiveJavaObject(o);
                    byteArrayToOctalString(bytes, builder);
                    break;
                case TIMESTAMP:
                    builder.append(((TimestampObjectInspector) oi).getPrimitiveJavaObject(o));
                    break;
                case BYTE:  /* TINYINT */
                    builder.append(new Short(((ByteObjectInspector) oi).get(o)));
                    break;
                default:
                    throw new UnsupportedTypeException(oi.getTypeName()
                            + " conversion is not supported by HiveColumnarSerdeResolver");
            }
        }
        firstColumn = false;
    }

    /*
     * Get the delimiter character from the URL, verify and store it.
     * Must be a single ascii character (same restriction as Hawq's)
     * If a hex representation was passed, convert it to its char. 
     */
    private void parseDelimiterChar(InputData input) {

        String userDelim = input.getUserProperty("DELIMITER");

        final int VALID_LENGTH = 1;
        final int VALID_LENGTH_HEX = 4;

        if (userDelim.startsWith("\\x")) {
            if (userDelim.length() != VALID_LENGTH_HEX) {
                throw new IllegalArgumentException("Invalid hexdecimal value for delimiter (got" + userDelim + ")");
            }
            delimiter = (char) Integer.parseInt(userDelim.substring(2, VALID_LENGTH_HEX), 16);
            if (delimiter > 0x7F) {
                throw new IllegalArgumentException("Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got non ASCII " + delimiter + ")");
            }
            return;
        }

        if (userDelim.length() != VALID_LENGTH) {
            throw new IllegalArgumentException("Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got " + userDelim + ")");
        }

        if (userDelim.charAt(0) > 0x7F) {
            throw new IllegalArgumentException("Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got non ASCII " + userDelim + ")");
        }

        delimiter = userDelim.charAt(0);
    }

    /*
     * transform a byte array into a string of octal codes in the form \\xyz\\xyz
     *
     * We double escape each char because it is required in postgres bytea for some bytes.
     * In the minimum all non-printables, backslash, null and single quote.
     * Easier to just escape everything
     * see http://www.postgresql.org/docs/9.0/static/datatype-binary.html
     *
     * Octal codes must be padded to 3 characters (001, 012)
     */
    private void byteArrayToOctalString(byte [] bytes, StringBuilder sb) {
        sb.ensureCapacity(sb.length() + (bytes.length * 5 /* characters per byte */));
        for (int b : bytes) {
            sb.append(String.format("\\\\%03o", b & 0xff));
        }
    }
}
