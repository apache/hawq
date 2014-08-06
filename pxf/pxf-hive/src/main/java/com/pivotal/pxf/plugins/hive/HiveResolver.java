package com.pivotal.pxf.plugins.hive;

import com.pivotal.pxf.api.*;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import org.apache.commons.lang.CharUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.pivotal.pxf.api.io.DataType.*;

/**
 * Class HiveResolver handles deserialization of records that were serialized
 * using Hadoop's Hive serialization framework.
 */
/*
TODO - remove SupressWarning once Hive resolves the problem described below
This line and the change of the deserialiazer member to Object instead of the original Deserializer...., All this changes stem from the same issue.
In 0.11.0 The API changed and all Serde types extend a new interface - AbstractSerde.
But this change was not adopted by the OrcSerde (which was also introduced in Hive 0.11.0).
In order to cope with this inconsistency... this bit of juggling has been necessary.
*/
@SuppressWarnings("deprecation")
public class HiveResolver extends Plugin implements ReadResolver {
    private SerDe deserializer;
    private List<OneField> partitionFields;
    private String serdeName;
    private String propsString;
    String partitionKeys;
    char delimiter;

    /**
     * Constructs the HiveResolver by parsing the userdata in the input
     * and obtaining the serde class name, the serde properties string
     * and the partition keys.
     *
     * @param input contains the Serde class name, the serde properties
     *              string and the partition keys
     */
    public HiveResolver(InputData input) throws Exception {
        super(input);

        parseUserData(input);
        initPartitionFields();
        initSerde(input);
    }

    @Override
    public List<OneField> getFields(OneRow onerow) throws Exception {
        List<OneField> record = new LinkedList<>();

        Object tuple = (deserializer).deserialize((Writable) onerow.getData());
        ObjectInspector oi = (deserializer).getObjectInspector();

        traverseTuple(tuple, oi, record);
        /* We follow Hive convention. Partition fields are always added at the end of the record */
        record.addAll(partitionFields);

        return record;
    }

    /* parse user data string (arrived from fragmenter) */
    void parseUserData(InputData input) throws Exception {
        final int EXPECTED_NUM_OF_TOKS = 4;

        String userData = new String(input.getFragmentUserData());
        String[] toks = userData.split(HiveDataFragmenter.HIVE_UD_DELIM);

        if (toks.length != EXPECTED_NUM_OF_TOKS) {
            throw new UserDataException("HiveResolver expected " + EXPECTED_NUM_OF_TOKS + " tokens, but got " + toks.length);
        }

        serdeName = toks[1];
        propsString = toks[2];
        partitionKeys = toks[3];
    }

    /* Get and init the deserializer for the records of this Hive data fragment */
    void initSerde(InputData inputData) throws Exception {
        Properties serdeProperties;

        Class<?> c = Class.forName(serdeName, true, JavaUtils.getClassLoader());
        deserializer = (SerDe) c.newInstance();
        serdeProperties = new Properties();
        ByteArrayInputStream inStream = new ByteArrayInputStream(propsString.getBytes());
        serdeProperties.load(inStream);
        deserializer.initialize(new JobConf(new Configuration(), HiveResolver.class), serdeProperties);
    }

    /* The partition fields are initialized one time base on userData provided by the fragmenter */
    void initPartitionFields() {
        partitionFields = new LinkedList<>();
        if (partitionKeys.equals(HiveDataFragmenter.HIVE_NO_PART_TBL)) {
            return;
        }

        String[] partitionLevels = partitionKeys.split(HiveDataFragmenter.HIVE_PARTITIONS_DELIM);
        for (String partLevel : partitionLevels) {
            String[] levelKey = partLevel.split(HiveDataFragmenter.HIVE_1_PART_DELIM);
            String type = levelKey[1];
            String val = levelKey[2];

            switch (type) {
                case serdeConstants.STRING_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, TEXT, val);
                    break;
                case serdeConstants.SMALLINT_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, SMALLINT, Short.parseShort(val));
                    break;
                case serdeConstants.INT_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, INTEGER, Integer.parseInt(val));
                    break;
                case serdeConstants.BIGINT_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, BIGINT, Long.parseLong(val));
                    break;
                case serdeConstants.FLOAT_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, REAL, Float.parseFloat(val));
                    break;
                case serdeConstants.DOUBLE_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, FLOAT8, Double.parseDouble(val));
                    break;
                case serdeConstants.TIMESTAMP_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, TIMESTAMP, Timestamp.valueOf(val));
                    break;
                case serdeConstants.DECIMAL_TYPE_NAME:
                    addOneFieldToRecord(partitionFields, NUMERIC, HiveDecimal.create(val).bigDecimalValue().toString());
                    break;
                default:
                    throw new UnsupportedTypeException("Unsupported partition type: " + type);
            }
        }
    }

    /* The partition fields are initialized one time base on userData provided by the fragmenter */
    int initPartitionFields(StringBuilder parts) {
        if (partitionKeys.equals(HiveDataFragmenter.HIVE_NO_PART_TBL)) {
            return 0;
        }
        String[] partitionLevels = partitionKeys.split(HiveDataFragmenter.HIVE_PARTITIONS_DELIM);
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
        return partitionLevels.length;
    }

    /*
     * If the object representing the whole record is null or if an object representing a composite sub-object (map, list,..)
     * is null - then BadRecordException will be thrown. If a primitive field value is null, then a null will appear for
     * the field in the record in the query result.
     */
    private void traverseTuple(Object obj, ObjectInspector objInspector, List<OneField> record) throws IOException, BadRecordException {
        ObjectInspector.Category category = objInspector.getCategory();
        if ((obj == null) && (category != ObjectInspector.Category.PRIMITIVE)) {
            throw new BadRecordException("NULL Hive composite object");
        }
        List<?> list;
        switch (category) {
            case PRIMITIVE:
                resolvePrimitive(obj, (PrimitiveObjectInspector) objInspector, record);
                break;
            case LIST:
                ListObjectInspector loi = (ListObjectInspector) objInspector;
                list = loi.getList(obj);
                ObjectInspector eoi = loi.getListElementObjectInspector();
                if (list == null) {
                    throw new BadRecordException("Illegal value NULL for Hive data type List");
                }
                for (Object object : list) {
                    traverseTuple(object, eoi, record);
                }
                break;
            case MAP:
                MapObjectInspector moi = (MapObjectInspector) objInspector;
                ObjectInspector koi = moi.getMapKeyObjectInspector();
                ObjectInspector voi = moi.getMapValueObjectInspector();
                Map<?, ?> map = moi.getMap(obj);
                if (map == null) {
                    throw new BadRecordException("Illegal value NULL for Hive data type Map");
                } else if (map.isEmpty()) {
                    traverseTuple(null, koi, record);
                    traverseTuple(null, voi, record);
                } else {
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        traverseTuple(entry.getKey(), koi, record);
                        traverseTuple(entry.getValue(), voi, record);
                    }
                }
                break;
            case STRUCT:
                StructObjectInspector soi = (StructObjectInspector) objInspector;
                List<? extends StructField> fields = soi.getAllStructFieldRefs();
                list = soi.getStructFieldsDataAsList(obj);
                if (list == null) {
                    throw new BadRecordException("Illegal value NULL for Hive data type Struct");
                }
                for (int i = 0; i < list.size(); i++) {
                    traverseTuple(list.get(i), fields.get(i).getFieldObjectInspector(), record);
                }
                break;
            case UNION:
                UnionObjectInspector uoi = (UnionObjectInspector) objInspector;
                List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
                if (ois == null) {
                    throw new BadRecordException("Illegal value NULL for Hive data type Union");
                }
                traverseTuple(uoi.getField(obj), ois.get(uoi.getTag(obj)), record);
                break;
            default:
                throw new UnsupportedTypeException("Unknown category type: " + objInspector.getCategory());
        }
    }

    private void resolvePrimitive(Object o, PrimitiveObjectInspector oi, List<OneField> record) throws IOException {
        Object val;
        switch (oi.getPrimitiveCategory()) {
            case BOOLEAN: {
                val = (o != null) ? ((BooleanObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, BOOLEAN, val);
                break;
            }
            case SHORT: {
                val = (o != null) ? ((ShortObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, SMALLINT, val);
                break;
            }
            case INT: {
                val = (o != null) ? ((IntObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, INTEGER, val);
                break;
            }
            case LONG: {
                val = (o != null) ? ((LongObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, BIGINT, val);
                break;
            }
            case FLOAT: {
                val = (o != null) ? ((FloatObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, REAL, val);
                break;
            }
            case DOUBLE: {
                val = (o != null) ? ((DoubleObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, FLOAT8, val);
                break;
            }
            case DECIMAL: {
                String sVal = null;
                if (o != null) {
                    HiveDecimal hd = ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o);
                    if (hd != null) {
                        BigDecimal bd = hd.bigDecimalValue();
                        sVal = bd.toString();
                    }
                }
                addOneFieldToRecord(record, NUMERIC, sVal);
                break;
            }
            case STRING: {
                val = (o != null) ? ((StringObjectInspector) oi).getPrimitiveJavaObject(o) : null;
                addOneFieldToRecord(record, TEXT, val);
                break;
            }
            case BINARY: {
                byte[] toEncode = null;
                if (o != null) {
                    BytesWritable bw = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o);
                    toEncode = new byte[bw.getLength()];
                    System.arraycopy(bw.getBytes(), 0, toEncode, 0, bw.getLength());
                }
                addOneFieldToRecord(record, BYTEA, toEncode);
                break;
            }
            case TIMESTAMP: {
                val = (o != null) ? ((TimestampObjectInspector) oi).getPrimitiveJavaObject(o) : null;
                addOneFieldToRecord(record, TIMESTAMP, val);
                break;
            }
            case BYTE: { /* TINYINT */
                val = (o != null) ? new Short(((ByteObjectInspector) oi).get(o)) : null;
                addOneFieldToRecord(record, SMALLINT, val);
                break;
            }
            default: {
                throw new UnsupportedTypeException(oi.getTypeName() + " conversion is not supported by " + getClass().getSimpleName());
            }
        }
    }

    private void addOneFieldToRecord(List<OneField> record, DataType gpdbWritableType, Object val) {
        record.add(new OneField(gpdbWritableType.getOID(), val));
    }

    /*
     * Get the delimiter character from the URL, verify and store it.
     * Must be a single ascii character (same restriction as Hawq's)
     * If a hex representation was passed, convert it to its char.
     */
    void parseDelimiterChar(InputData input) {

        String userDelim = input.getUserProperty("DELIMITER");

        final int VALID_LENGTH = 1;
        final int VALID_LENGTH_HEX = 4;

        if (userDelim.startsWith("\\x")) {  // hexadecimal sequence

            if (userDelim.length() != VALID_LENGTH_HEX) {
                throw new IllegalArgumentException("Invalid hexdecimal value for delimiter (got" + userDelim + ")");
            }

            delimiter = (char) Integer.parseInt(userDelim.substring(2, VALID_LENGTH_HEX), 16);

            if (!CharUtils.isAscii(delimiter)) {
                throw new IllegalArgumentException("Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got non ASCII " + delimiter + ")");
            }

            return;
        }

        if (userDelim.length() != VALID_LENGTH) {
            throw new IllegalArgumentException("Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got " + userDelim + ")");
        }

        if (!CharUtils.isAscii(userDelim.charAt(0))) {
            throw new IllegalArgumentException("Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got non ASCII " + userDelim + ")");
        }

        delimiter = userDelim.charAt(0);
    }
}
