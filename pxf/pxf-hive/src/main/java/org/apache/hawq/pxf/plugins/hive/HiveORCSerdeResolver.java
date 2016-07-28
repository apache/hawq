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

import org.apache.commons.lang.CharUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hawq.pxf.api.BadRecordException;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Utilities;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import static org.apache.hawq.pxf.api.io.DataType.*;
import static org.apache.hawq.pxf.api.io.DataType.DATE;
import static org.apache.hawq.pxf.api.io.DataType.SMALLINT;

/**
 * Specialized HiveResolver for a Hive table stored as RC file.
 * Use together with HiveInputFormatFragmenter/HiveRCFileAccessor.
 */
public class HiveORCSerdeResolver extends HiveResolver {
    private static final Log LOG = LogFactory.getLog(HiveORCSerdeResolver.class);
    private OrcSerde deserializer;
    private boolean firstColumn;
    private StringBuilder builder;
    private StringBuilder parts;
    private int numberOfPartitions;
    private HiveInputFormatFragmenter.PXF_HIVE_SERDES serdeType;
    private static final String MAPKEY_DELIM = ":";
    private static final String COLLECTION_DELIM = ",";
    private String collectionDelim;
    private String mapkeyDelim;

    public HiveORCSerdeResolver(InputData input) throws Exception {
        super(input);
    }

    /* read the data supplied by the fragmenter: inputformat name, serde name, partition keys */
    @Override
    void parseUserData(InputData input) throws Exception {
        String[] toks = HiveInputFormatFragmenter.parseToks(input);
        String serdeEnumStr = toks[HiveInputFormatFragmenter.TOK_SERDE];
        if (serdeEnumStr.equals(HiveInputFormatFragmenter.PXF_HIVE_SERDES.ORC_SERDE.name())) {
            serdeType = HiveInputFormatFragmenter.PXF_HIVE_SERDES.ORC_SERDE;
        } else {
            throw new UnsupportedTypeException("Unsupported Hive Serde: " + serdeEnumStr);
        }
        parts = new StringBuilder();
        partitionKeys = toks[HiveInputFormatFragmenter.TOK_KEYS];
        parseDelimiterChar(input);
        collectionDelim = input.getUserProperty("COLLECTION_DELIM") == null ? COLLECTION_DELIM
                : input.getUserProperty("COLLECTION_DELIM");
        mapkeyDelim = input.getUserProperty("MAPKEY_DELIM") == null ? MAPKEY_DELIM
                : input.getUserProperty("MAPKEY_DELIM");
    }

    @Override
    void initPartitionFields() {
        numberOfPartitions = initPartitionFields(parts);
    }

    /**
     * getFields returns a singleton list of OneField item.
     * OneField item contains two fields: an integer representing the VARCHAR type and a Java
     * Object representing the field value.
     */
    @Override
    public List<OneField> getFields(OneRow onerow) throws Exception {

        Object tuple = deserializer.deserialize((Writable) onerow.getData());
        // Each Hive record is a Struct
        StructObjectInspector soi = (StructObjectInspector) deserializer.getObjectInspector();
        List<OneField> record = traverseStruct(tuple, soi, false);

        return record;

    }

    /*
     * Get and init the deserializer for the records of this Hive data fragment.
     * Suppress Warnings added because deserializer.initialize is an abstract function that is deprecated
     * but its implementations (ColumnarSerDe, LazyBinaryColumnarSerDe) still use the deprecated interface.
     */
    @SuppressWarnings("deprecation")
	@Override
    void initSerde(InputData input) throws Exception {
        Properties serdeProperties = new Properties();
        int numberOfDataColumns = input.getColumns() - numberOfPartitions;

        LOG.debug("Serde number of columns is " + numberOfDataColumns);

        StringBuilder columnNames = new StringBuilder(numberOfDataColumns * 2); // column + delimiter
        StringBuilder columnTypes = new StringBuilder(numberOfDataColumns * 2); // column + delimiter
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

        if (serdeType == HiveInputFormatFragmenter.PXF_HIVE_SERDES.ORC_SERDE) {
            deserializer = new OrcSerde();
        } else {
            throw new UnsupportedTypeException("Unsupported Hive Serde: " + serdeType.name()); /* we should not get here */
        }

        deserializer.initialize(new JobConf(new Configuration(), HiveORCSerdeResolver.class), serdeProperties);
    }

    /*
     * If the object representing the whole record is null or if an object
     * representing a composite sub-object (map, list,..) is null - then
     * BadRecordException will be thrown. If a primitive field value is null,
     * then a null will appear for the field in the record in the query result.
     */
    private void traverseTuple(Object obj, ObjectInspector objInspector,
                               List<OneField> record, boolean toFlatten)
            throws IOException, BadRecordException {
        ObjectInspector.Category category = objInspector.getCategory();
        if ((obj == null) && (category != ObjectInspector.Category.PRIMITIVE)) {
            throw new BadRecordException("NULL Hive composite object");
        }
        switch (category) {
            case PRIMITIVE:
                resolvePrimitive(obj, (PrimitiveObjectInspector) objInspector,
                        record, toFlatten);
                break;
            case LIST:
                List<OneField> listRecord = traverseList(obj,
                        (ListObjectInspector) objInspector);
                addOneFieldToRecord(record, TEXT, String.format("[%s]",
                        HdfsUtilities.toString(listRecord, collectionDelim)));
                break;
            case MAP:
                List<OneField> mapRecord = traverseMap(obj,
                        (MapObjectInspector) objInspector);
                addOneFieldToRecord(record, TEXT, String.format("{%s}",
                        HdfsUtilities.toString(mapRecord, collectionDelim)));
                break;
            case STRUCT:
                List<OneField> structRecord = traverseStruct(obj,
                        (StructObjectInspector) objInspector, true);
                addOneFieldToRecord(record, TEXT, String.format("{%s}",
                        HdfsUtilities.toString(structRecord, collectionDelim)));
                break;
            case UNION:
                List<OneField> unionRecord = traverseUnion(obj,
                        (UnionObjectInspector) objInspector);
                addOneFieldToRecord(record, TEXT, String.format("[%s]",
                        HdfsUtilities.toString(unionRecord, collectionDelim)));
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

    private List<OneField> traverseStruct(Object struct,
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
        for (int i = 0; i < structFields.size(); i++) {
            if (toFlatten) {
                complexRecord.add(new OneField(TEXT.getOID(), String.format(
                        "\"%s\"", fields.get(i).getFieldName())));
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
