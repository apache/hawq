package org.apache.hawq.pxf.plugins.hive;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.hadoop.io.BytesWritable;
import org.apache.hawq.pxf.api.*;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Plugin;
import org.apache.hawq.pxf.api.utilities.Utilities;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.apache.commons.lang.CharUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hawq.pxf.api.io.DataType.*;

/**
 * Class HiveResolver handles deserialization of records that were serialized
 * using Hadoop's Hive serialization framework.
 */
/*
 * TODO - remove SupressWarning once Hive resolves the problem described below
 * This line and the change of the deserialiazer member to Object instead of the
 * original Deserializer...., All this changes stem from the same issue. In
 * 0.11.0 The API changed and all Serde types extend a new interface -
 * AbstractSerde. But this change was not adopted by the OrcSerde (which was
 * also introduced in Hive 0.11.0). In order to cope with this inconsistency...
 * this bit of juggling has been necessary.
 */
@SuppressWarnings("deprecation")
public class HiveResolver extends Plugin implements ReadResolver {
    private static final Log LOG = LogFactory.getLog(HiveResolver.class);
    protected static final String MAPKEY_DELIM = ":";
    protected static final String COLLECTION_DELIM = ",";
    protected String collectionDelim;
    protected String mapkeyDelim;
    private SerDe deserializer;
    private List<OneField> partitionFields;
    private String serdeName;
    private String propsString;
    String partitionKeys;
    char delimiter;
    String nullChar = "\\N";
    private Configuration conf;
    private String hiveDefaultPartName;

    /**
     * Constructs the HiveResolver by parsing the userdata in the input and
     * obtaining the serde class name, the serde properties string and the
     * partition keys.
     *
     * @param input contains the Serde class name, the serde properties string
     *            and the partition keys
     * @throws Exception if user data was wrong or serde failed to be
     *             instantiated
     */
    public HiveResolver(InputData input) throws Exception {
        super(input);

        conf = new Configuration();
        hiveDefaultPartName = HiveConf.getVar(conf,
                HiveConf.ConfVars.DEFAULTPARTITIONNAME);
        LOG.debug("Hive's default partition name is " + hiveDefaultPartName);

        parseUserData(input);
        initPartitionFields();
        initSerde(input);
    }

    @Override
    public List<OneField> getFields(OneRow onerow) throws Exception {
        Object tuple = deserializer.deserialize((Writable) onerow.getData());
        // Each Hive record is a Struct
        StructObjectInspector soi = (StructObjectInspector) deserializer.getObjectInspector();
        List<OneField> record = traverseStruct(tuple, soi, false);
        /*
         * We follow Hive convention. Partition fields are always added at the
         * end of the record
         */
        record.addAll(partitionFields);

        return record;
    }

    /* Parses user data string (arrived from fragmenter). */
    void parseUserData(InputData input) throws Exception {
        final int EXPECTED_NUM_OF_TOKS = 5;

        String userData = new String(input.getFragmentUserData());
        String[] toks = userData.split(HiveDataFragmenter.HIVE_UD_DELIM);

        if (toks.length != EXPECTED_NUM_OF_TOKS) {
            throw new UserDataException("HiveResolver expected "
                    + EXPECTED_NUM_OF_TOKS + " tokens, but got " + toks.length);
        }

        serdeName = toks[1];
        propsString = toks[2];
        partitionKeys = toks[3];

        collectionDelim = input.getUserProperty("COLLECTION_DELIM") == null ? COLLECTION_DELIM
                : input.getUserProperty("COLLECTION_DELIM");
        mapkeyDelim = input.getUserProperty("MAPKEY_DELIM") == null ? MAPKEY_DELIM
                : input.getUserProperty("MAPKEY_DELIM");
    }

    /*
     * Gets and init the deserializer for the records of this Hive data
     * fragment.
     */
    void initSerde(InputData inputData) throws Exception {
        Properties serdeProperties;

        Class<?> c = Class.forName(serdeName, true, JavaUtils.getClassLoader());
        deserializer = (SerDe) c.newInstance();
        serdeProperties = new Properties();
        ByteArrayInputStream inStream = new ByteArrayInputStream(
                propsString.getBytes());
        serdeProperties.load(inStream);
        deserializer.initialize(new JobConf(conf, HiveResolver.class),
                serdeProperties);
    }

    /*
     * The partition fields are initialized one time base on userData provided
     * by the fragmenter.
     */
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
            DataType convertedType;
            Object convertedValue = null;
            boolean isDefaultPartition = false;

            LOG.debug("Partition type: " + type + ", value: " + val);
            // check if value is default partition
            isDefaultPartition = isDefaultPartition(type, val);
            // ignore the type's parameters
            String typeName = type.replaceAll("\\(.*\\)", "");

            switch (typeName) {
                case serdeConstants.STRING_TYPE_NAME:
                    convertedType = TEXT;
                    convertedValue = isDefaultPartition ? null : val;
                    break;
                case serdeConstants.BOOLEAN_TYPE_NAME:
                    convertedType = BOOLEAN;
                    convertedValue = isDefaultPartition ? null
                            : Boolean.valueOf(val);
                    break;
                case serdeConstants.TINYINT_TYPE_NAME:
                case serdeConstants.SMALLINT_TYPE_NAME:
                    convertedType = SMALLINT;
                    convertedValue = isDefaultPartition ? null
                            : Short.parseShort(val);
                    break;
                case serdeConstants.INT_TYPE_NAME:
                    convertedType = INTEGER;
                    convertedValue = isDefaultPartition ? null
                            : Integer.parseInt(val);
                    break;
                case serdeConstants.BIGINT_TYPE_NAME:
                    convertedType = BIGINT;
                    convertedValue = isDefaultPartition ? null
                            : Long.parseLong(val);
                    break;
                case serdeConstants.FLOAT_TYPE_NAME:
                    convertedType = REAL;
                    convertedValue = isDefaultPartition ? null
                            : Float.parseFloat(val);
                    break;
                case serdeConstants.DOUBLE_TYPE_NAME:
                    convertedType = FLOAT8;
                    convertedValue = isDefaultPartition ? null
                            : Double.parseDouble(val);
                    break;
                case serdeConstants.TIMESTAMP_TYPE_NAME:
                    convertedType = TIMESTAMP;
                    convertedValue = isDefaultPartition ? null
                            : Timestamp.valueOf(val);
                    break;
                case serdeConstants.DATE_TYPE_NAME:
                    convertedType = DATE;
                    convertedValue = isDefaultPartition ? null
                            : Date.valueOf(val);
                    break;
                case serdeConstants.DECIMAL_TYPE_NAME:
                    convertedType = NUMERIC;
                    convertedValue = isDefaultPartition ? null
                            : HiveDecimal.create(val).bigDecimalValue().toString();
                    break;
                case serdeConstants.VARCHAR_TYPE_NAME:
                    convertedType = VARCHAR;
                    convertedValue = isDefaultPartition ? null : val;
                    break;
                case serdeConstants.CHAR_TYPE_NAME:
                    convertedType = BPCHAR;
                    convertedValue = isDefaultPartition ? null : val;
                    break;
                case serdeConstants.BINARY_TYPE_NAME:
                    convertedType = BYTEA;
                    convertedValue = isDefaultPartition ? null : val.getBytes();
                    break;
                default:
                    throw new UnsupportedTypeException(
                            "Unsupported partition type: " + type);
            }
            addOneFieldToRecord(partitionFields, convertedType, convertedValue);
        }
    }

    /*
     * The partition fields are initialized one time based on userData provided
     * by the fragmenter.
     */
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

            if (isDefaultPartition(type, val)) {
                parts.append(nullChar);
            } else {
                // ignore the type's parameters
                String typeName = type.replaceAll("\\(.*\\)", "");
                switch (typeName) {
                    case serdeConstants.STRING_TYPE_NAME:
                    case serdeConstants.VARCHAR_TYPE_NAME:
                    case serdeConstants.CHAR_TYPE_NAME:
                        parts.append(val);
                        break;
                    case serdeConstants.BOOLEAN_TYPE_NAME:
                        parts.append(Boolean.parseBoolean(val));
                        break;
                    case serdeConstants.TINYINT_TYPE_NAME:
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
                    case serdeConstants.DATE_TYPE_NAME:
                        parts.append(Date.valueOf(val));
                        break;
                    case serdeConstants.DECIMAL_TYPE_NAME:
                        parts.append(HiveDecimal.create(val).bigDecimalValue());
                        break;
                    case serdeConstants.BINARY_TYPE_NAME:
                        Utilities.byteArrayToOctalString(val.getBytes(), parts);
                        break;
                    default:
                        throw new UnsupportedTypeException(
                                "Unsupported partition type: " + type);
                }
            }
        }
        return partitionLevels.length;
    }

    /**
     * Returns true if the partition value is Hive's default partition name
     * (defined in hive.exec.default.partition.name).
     *
     * @param partitionType partition field type
     * @param partitionValue partition value
     * @return true if the partition value is Hive's default partition
     */
    private boolean isDefaultPartition(String partitionType,
                                       String partitionValue) {
        boolean isDefaultPartition = false;
        if (hiveDefaultPartName.equals(partitionValue)) {
            LOG.debug("partition " + partitionType
                    + " is hive default partition (value " + partitionValue
                    + "), converting field to NULL");
            isDefaultPartition = true;
        }
        return isDefaultPartition;
    }

    /*
     * If the object representing the whole record is null or if an object
     * representing a composite sub-object (map, list,..) is null - then
     * BadRecordException will be thrown. If a primitive field value is null,
     * then a null will appear for the field in the record in the query result.
     * flatten is true only when we are dealing with a non primitive field
     */
    private void traverseTuple(Object obj, ObjectInspector objInspector,
                               List<OneField> record, boolean toFlatten)
            throws IOException, BadRecordException {
        ObjectInspector.Category category = objInspector.getCategory();
        switch (category) {
            case PRIMITIVE:
                resolvePrimitive(obj, (PrimitiveObjectInspector) objInspector,
                        record, toFlatten);
                break;
            case LIST:
                if(obj == null) {
                    addOneFieldToRecord(record, TEXT, null);
                } else {
                    List<OneField> listRecord = traverseList(obj,
                            (ListObjectInspector) objInspector);
                    addOneFieldToRecord(record, TEXT, String.format("[%s]",
                            HdfsUtilities.toString(listRecord, collectionDelim)));
                }
                break;
            case MAP:
                if(obj == null) {
                    addOneFieldToRecord(record, TEXT, null);
                } else {
                    List<OneField> mapRecord = traverseMap(obj,
                            (MapObjectInspector) objInspector);
                    addOneFieldToRecord(record, TEXT, String.format("{%s}",
                            HdfsUtilities.toString(mapRecord, collectionDelim)));
                }
                break;
            case STRUCT:
                if(obj == null) {
                    addOneFieldToRecord(record, TEXT, null);
                } else {
                    List<OneField> structRecord = traverseStruct(obj,
                            (StructObjectInspector) objInspector, true);
                    addOneFieldToRecord(record, TEXT, String.format("{%s}",
                            HdfsUtilities.toString(structRecord, collectionDelim)));
                }
                break;
            case UNION:
                if(obj == null) {
                    addOneFieldToRecord(record, TEXT, null);
                } else {
                    List<OneField> unionRecord = traverseUnion(obj,
                            (UnionObjectInspector) objInspector);
                    addOneFieldToRecord(record, TEXT, String.format("[%s]",
                            HdfsUtilities.toString(unionRecord, collectionDelim)));
                }
                break;
            default:
                throw new UnsupportedTypeException("Unknown category type: "
                        + objInspector.getCategory());
        }
    }

    private List<OneField> traverseUnion(Object obj, UnionObjectInspector uoi)
            throws BadRecordException, IOException {
        List<OneField> unionRecord = new LinkedList<>();
        List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
        if (ois == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type Union");
        }
        traverseTuple(uoi.getField(obj), ois.get(uoi.getTag(obj)), unionRecord,
                true);
        return unionRecord;
    }

    private List<OneField> traverseList(Object obj, ListObjectInspector loi)
            throws BadRecordException, IOException {
        List<OneField> listRecord = new LinkedList<>();
        List<?> list = loi.getList(obj);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type List");
        }
        for (Object object : list) {
            traverseTuple(object, eoi, listRecord, true);
        }
        return listRecord;
    }

    protected List<OneField> traverseStruct(Object struct,
                                          StructObjectInspector soi,
                                          boolean toFlatten)
            throws BadRecordException, IOException {
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> structFields = soi.getStructFieldsDataAsList(struct);
        if (structFields == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type Struct");
        }
        List<OneField> structRecord = new LinkedList<>();
        List<OneField> complexRecord = new LinkedList<>();
        List<ColumnDescriptor> colData = inputData.getTupleDescription();
        for (int i = 0; i < structFields.size(); i++) {
            if (toFlatten) {
                complexRecord.add(new OneField(TEXT.getOID(), String.format(
                        "\"%s\"", fields.get(i).getFieldName())));
            } else if (!colData.get(i).isProjected()) {
                // Non-projected fields will be sent as null values.
                // This case is invoked only in the top level of fields and
                // not when interpreting fields of type struct.
                traverseTuple(null, fields.get(i).getFieldObjectInspector(),
                        complexRecord, toFlatten);
                continue;
            }
            traverseTuple(structFields.get(i),
                    fields.get(i).getFieldObjectInspector(), complexRecord,
                    toFlatten);
            if (toFlatten) {
                addOneFieldToRecord(structRecord, TEXT,
                        HdfsUtilities.toString(complexRecord, mapkeyDelim));
                complexRecord.clear();
            }
        }
        return toFlatten ? structRecord : complexRecord;
    }

    private List<OneField> traverseMap(Object obj, MapObjectInspector moi)
            throws BadRecordException, IOException {
        List<OneField> complexRecord = new LinkedList<>();
        List<OneField> mapRecord = new LinkedList<>();
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();
        Map<?, ?> map = moi.getMap(obj);
        if (map == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type Map");
        } else if (map.isEmpty()) {
            traverseTuple(null, koi, complexRecord, true);
            traverseTuple(null, voi, complexRecord, true);
            addOneFieldToRecord(mapRecord, TEXT,
                    HdfsUtilities.toString(complexRecord, mapkeyDelim));
        } else {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                traverseTuple(entry.getKey(), koi, complexRecord, true);
                traverseTuple(entry.getValue(), voi, complexRecord, true);
                addOneFieldToRecord(mapRecord, TEXT,
                        HdfsUtilities.toString(complexRecord, mapkeyDelim));
                complexRecord.clear();
            }
        }
        return mapRecord;
    }

    private void resolvePrimitive(Object o, PrimitiveObjectInspector oi,
                                  List<OneField> record, boolean toFlatten)
            throws IOException {
        Object val;
        switch (oi.getPrimitiveCategory()) {
            case BOOLEAN: {
                val = (o != null) ? ((BooleanObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, BOOLEAN, val);
                break;
            }
            case SHORT: {
                if(o == null) {
                    val = null;
                } else if( o.getClass().getSimpleName().equals("ByteWritable") ) {
                    val = new Short(((ByteWritable) o).get());
                } else {
                    val = ((ShortObjectInspector) oi).get(o);
                }
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
                val = (o != null) ? ((StringObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, TEXT,
                        toFlatten ? String.format("\"%s\"", val) : val);
                break;
            }
            case VARCHAR:
                val = (o != null) ? ((HiveVarcharObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, VARCHAR,
                        toFlatten ? String.format("\"%s\"", val) : val);
                break;
            case CHAR:
                val = (o != null) ? ((HiveCharObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, BPCHAR,
                        toFlatten ? String.format("\"%s\"", val) : val);
                break;
            case BINARY: {
                byte[] toEncode = null;
                if (o != null) {
                    BytesWritable bw = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o);
                    toEncode = new byte[bw.getLength()];
                    System.arraycopy(bw.getBytes(), 0, toEncode, 0,
                            bw.getLength());
                }
                addOneFieldToRecord(record, BYTEA, toEncode);
                break;
            }
            case TIMESTAMP: {
                val = (o != null) ? ((TimestampObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, TIMESTAMP, val);
                break;
            }
            case DATE:
                val = (o != null) ? ((DateObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, DATE, val);
                break;
            case BYTE: { /* TINYINT */
                val = (o != null) ? new Short(((ByteObjectInspector) oi).get(o))
                        : null;
                addOneFieldToRecord(record, SMALLINT, val);
                break;
            }
            default: {
                throw new UnsupportedTypeException(oi.getTypeName()
                        + " conversion is not supported by "
                        + getClass().getSimpleName());
            }
        }
    }

    private void addOneFieldToRecord(List<OneField> record,
                                     DataType gpdbWritableType, Object val) {
        record.add(new OneField(gpdbWritableType.getOID(), val));
    }

    /*
     * Gets the delimiter character from the URL, verify and store it. Must be a
     * single ascii character (same restriction as Hawq's). If a hex
     * representation was passed, convert it to its char.
     */
    void parseDelimiterChar(InputData input) {

        String userDelim = input.getUserProperty("DELIMITER");

        if (userDelim == null) {
            throw new IllegalArgumentException("DELIMITER is a required option");
        }

        final int VALID_LENGTH = 1;
        final int VALID_LENGTH_HEX = 4;

        if (userDelim.startsWith("\\x")) { // hexadecimal sequence

            if (userDelim.length() != VALID_LENGTH_HEX) {
                throw new IllegalArgumentException(
                        "Invalid hexdecimal value for delimiter (got"
                                + userDelim + ")");
            }

            delimiter = (char) Integer.parseInt(
                    userDelim.substring(2, VALID_LENGTH_HEX), 16);

            if (!CharUtils.isAscii(delimiter)) {
                throw new IllegalArgumentException(
                        "Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got non ASCII "
                                + delimiter + ")");
            }

            return;
        }

        if (userDelim.length() != VALID_LENGTH) {
            throw new IllegalArgumentException(
                    "Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got "
                            + userDelim + ")");
        }

        if (!CharUtils.isAscii(userDelim.charAt(0))) {
            throw new IllegalArgumentException(
                    "Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got non ASCII "
                            + userDelim + ")");
        }

        delimiter = userDelim.charAt(0);
    }
}
