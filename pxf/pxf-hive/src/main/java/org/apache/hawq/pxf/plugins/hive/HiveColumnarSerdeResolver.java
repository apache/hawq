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

import org.apache.hawq.pxf.api.BadRecordException;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.api.utilities.Utilities;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.hawq.pxf.api.io.DataType.VARCHAR;

/**
 * Specialized HiveResolver for a Hive table stored as RC file.
 * Use together with HiveInputFormatFragmenter/HiveRCFileAccessor.
 */
public class HiveColumnarSerdeResolver extends HiveResolver {
    private static final Log LOG = LogFactory.getLog(HiveColumnarSerdeResolver.class);
    private ColumnarSerDeBase deserializer;
    private boolean firstColumn;
    private StringBuilder builder;
    private StringBuilder parts;
    private int numberOfPartitions;
    private HiveInputFormatFragmenter.PXF_HIVE_SERDES serdeType;

    public HiveColumnarSerdeResolver(InputData input) throws Exception {
        super(input);
    }

    /* read the data supplied by the fragmenter: inputformat name, serde name, partition keys */
    @Override
    void parseUserData(InputData input) throws Exception {
        String[] toks = HiveInputFormatFragmenter.parseToks(input);
        String serdeEnumStr = toks[HiveInputFormatFragmenter.TOK_SERDE];
        if (serdeEnumStr.equals(HiveInputFormatFragmenter.PXF_HIVE_SERDES.COLUMNAR_SERDE.name())) {
            serdeType = HiveInputFormatFragmenter.PXF_HIVE_SERDES.COLUMNAR_SERDE;
        } else if (serdeEnumStr.equals(HiveInputFormatFragmenter.PXF_HIVE_SERDES.LAZY_BINARY_COLUMNAR_SERDE.name())) {
            serdeType = HiveInputFormatFragmenter.PXF_HIVE_SERDES.LAZY_BINARY_COLUMNAR_SERDE;
        }
        else {
            throw new UnsupportedTypeException("Unsupported Hive Serde: " + serdeEnumStr);
        }
        parts = new StringBuilder();
        partitionKeys = toks[HiveInputFormatFragmenter.TOK_KEYS];
        parseDelimiterChar(input);
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
        firstColumn = true;
        builder = new StringBuilder();
        Object tuple = deserializer.deserialize((Writable) onerow.getData());
        ObjectInspector oi = deserializer.getObjectInspector();

        traverseTuple(tuple, oi);
        /* We follow Hive convention. Partition fields are always added at the end of the record */
        builder.append(parts);
        return Collections.singletonList(new OneField(VARCHAR.getOID(), builder.toString()));
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
        String delim = ",";
        for (int i = 0; i < numberOfDataColumns; i++) {
            ColumnDescriptor column = input.getColumn(i);
            String columnName = column.columnName();
            String columnType = HiveUtilities.toCompatibleHiveType(DataType.get(column.columnTypeCode()),column.columnTypeModifiers());
            if(i > 0) {
                columnNames.append(delim);
                columnTypes.append(delim);
            }
            columnNames.append(columnName);
            columnTypes.append(columnType);
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

    /**
     * Handle a Hive record.
     * Supported object categories:
     * Primitive - including NULL
     * Struct (used by ColumnarSerDe to store primitives) - cannot be NULL
     * <p/>
     * Any other category will throw UnsupportedTypeException
     */
    private void traverseTuple(Object obj, ObjectInspector objInspector) throws IOException, BadRecordException {
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

    private void resolvePrimitive(Object o, PrimitiveObjectInspector oi) throws IOException {

        if (!firstColumn) {
            builder.append(delimiter);
        }

        if (o == null) {
            builder.append(nullChar);
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
                    Utilities.byteArrayToOctalString(bytes, builder);
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
}
