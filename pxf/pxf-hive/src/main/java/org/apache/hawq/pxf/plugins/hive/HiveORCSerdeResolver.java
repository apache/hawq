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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hawq.pxf.api.OneField;
import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities;

import java.util.*;

/**
 * Specialized HiveResolver for a Hive table stored as RC file.
 * Use together with HiveInputFormatFragmenter/HiveRCFileAccessor.
 */
public class HiveORCSerdeResolver extends HiveResolver {
    private static final Log LOG = LogFactory.getLog(HiveORCSerdeResolver.class);
    private OrcSerde deserializer;
    private StringBuilder parts;
    private int numberOfPartitions;
    private HiveInputFormatFragmenter.PXF_HIVE_SERDES serdeType;

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
        partitionKeys = toks[HiveInputFormatFragmenter.TOK_KEYS];
        parseDelimiterChar(input);
        collectionDelim = input.getUserProperty("COLLECTION_DELIM") == null ? COLLECTION_DELIM
                : input.getUserProperty("COLLECTION_DELIM");
        mapkeyDelim = input.getUserProperty("MAPKEY_DELIM") == null ? MAPKEY_DELIM
                : input.getUserProperty("MAPKEY_DELIM");
    }

    @Override
    void initPartitionFields() {
        parts = new StringBuilder();
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
        String delim = ",";
        for (int i = 0; i < numberOfDataColumns; i++) {
            ColumnDescriptor column = input.getColumn(i);
            String columnName = column.columnName();
            String columnType = HiveUtilities.toCompatibleHiveType(DataType.get(column.columnTypeCode()), column.columnTypeModifiers());
            if(i > 0) {
                columnNames.append(delim);
                columnTypes.append(delim);
            }
            columnNames.append(columnName);
            columnTypes.append(columnType);
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

}
