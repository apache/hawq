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
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities.PXF_HIVE_SERDES;

import java.util.*;

/**
 * Specialized HiveResolver for a Hive table stored as RC file.
 * Use together with HiveInputFormatFragmenter/HiveRCFileAccessor.
 */
public class HiveORCSerdeResolver extends HiveResolver {
    private static final Log LOG = LogFactory.getLog(HiveORCSerdeResolver.class);
    private HiveUtilities.PXF_HIVE_SERDES serdeType;
    private String typesString;

    public HiveORCSerdeResolver(InputData input) throws Exception {
        super(input);
    }

    /* read the data supplied by the fragmenter: inputformat name, serde name, partition keys */
    @Override
    void parseUserData(InputData input) throws Exception {
        HiveUserData hiveUserData = HiveUtilities.parseHiveUserData(input, HiveUtilities.PXF_HIVE_SERDES.ORC_SERDE);
        serdeType = PXF_HIVE_SERDES.getPxfHiveSerde(hiveUserData.getSerdeClassName());
        partitionKeys = hiveUserData.getPartitionKeys();
        typesString = hiveUserData.getColTypes();
        collectionDelim = input.getUserProperty("COLLECTION_DELIM") == null ? COLLECTION_DELIM
                : input.getUserProperty("COLLECTION_DELIM");
        mapkeyDelim = input.getUserProperty("MAPKEY_DELIM") == null ? MAPKEY_DELIM
                : input.getUserProperty("MAPKEY_DELIM");
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
        int numberOfDataColumns = input.getColumns() - getNumberOfPartitions();

        LOG.debug("Serde number of columns is " + numberOfDataColumns);

        StringBuilder columnNames = new StringBuilder(numberOfDataColumns * 2); // column + delimiter
        StringBuilder columnTypes = new StringBuilder(numberOfDataColumns * 2); // column + delimiter
        String[] cols = typesString.split(":");
        String[] hiveColTypes = new String[numberOfDataColumns];
        parseColTypes(cols, hiveColTypes);

        String delim = ",";
        for (int i = 0; i < numberOfDataColumns; i++) {
            ColumnDescriptor column = input.getColumn(i);
            String columnName = column.columnName();
            String columnType = HiveUtilities.toCompatibleHiveType(DataType.get(column.columnTypeCode()), column.columnTypeModifiers());
            //Complex Types will have a mismatch between Hive and Hawq type
            if (!columnType.equals(hiveColTypes[i])) {
                columnType = hiveColTypes[i];
            }
            if(i > 0) {
                columnNames.append(delim);
                columnTypes.append(delim);
            }
            columnNames.append(columnName);
            columnTypes.append(columnType);
        }
        serdeProperties.put(serdeConstants.LIST_COLUMNS, columnNames.toString());
        serdeProperties.put(serdeConstants.LIST_COLUMN_TYPES, columnTypes.toString());

        deserializer = HiveUtilities.createDeserializer(serdeType, HiveUtilities.PXF_HIVE_SERDES.ORC_SERDE);
        deserializer.initialize(new JobConf(new Configuration(), HiveORCSerdeResolver.class), serdeProperties);
    }

    private void parseColTypes(String[] cols, String[] output) {
        int i = 0;
        StringBuilder structTypeBuilder = new StringBuilder();
        boolean inStruct = false;
        for (String str : cols) {
            if (str.contains("struct")) {
                structTypeBuilder = new StringBuilder();
                inStruct = true;
                structTypeBuilder.append(str);
            } else if (inStruct) {
                structTypeBuilder.append(':');
                structTypeBuilder.append(str);
                if (str.contains(">")) {
                    inStruct = false;
                    output[i++] = structTypeBuilder.toString();
                }
            } else {
                output[i++] = str;
            }
        }
    }
}
