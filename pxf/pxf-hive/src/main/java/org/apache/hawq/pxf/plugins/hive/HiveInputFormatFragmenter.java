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


import org.apache.hawq.pxf.api.FragmentsStats;
import org.apache.hawq.pxf.api.UnsupportedTypeException;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.io.DataType;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hive.utilities.EnumHiveToHawqType;
import org.apache.hawq.pxf.plugins.hive.utilities.HiveUtilities;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Arrays;
import java.util.List;

/**
 * Specialized Hive fragmenter for RC and Text files tables. Unlike the
 * {@link HiveDataFragmenter}, this class does not send the serde properties to
 * the accessor/resolvers. This is done to avoid memory explosion in Hawq. For
 * RC use together with {@link HiveRCFileAccessor}/
 * {@link HiveColumnarSerdeResolver}. For Text use together with
 * {@link HiveLineBreakAccessor}/{@link HiveStringPassResolver}. <br>
 * Given a Hive table and its partitions, divide the data into fragments (here a
 * data fragment is actually a HDFS file block) and return a list of them. Each
 * data fragment will contain the following information:
 * <ol>
 * <li>sourceName: full HDFS path to the data file that this data fragment is
 * part of</li>
 * <li>hosts: a list of the datanode machines that hold a replica of this block</li>
 * <li>userData: inputformat name, serde names and partition keys</li>
 * </ol>
 */
public class HiveInputFormatFragmenter extends HiveDataFragmenter {
    private static final Log LOG = LogFactory.getLog(HiveInputFormatFragmenter.class);
    private static final int EXPECTED_NUM_OF_TOKS = 3;
    public static final int TOK_SERDE = 0;
    public static final int TOK_KEYS = 1;
    public static final int TOK_FILTER_DONE = 2;

    /** Defines the Hive input formats currently supported in pxf */
    public enum PXF_HIVE_INPUT_FORMATS {
        RC_FILE_INPUT_FORMAT,
        TEXT_FILE_INPUT_FORMAT,
        ORC_FILE_INPUT_FORMAT
    }

    /** Defines the Hive serializers (serde classes) currently supported in pxf */
    public enum PXF_HIVE_SERDES {
        COLUMNAR_SERDE,
        LAZY_BINARY_COLUMNAR_SERDE,
        LAZY_SIMPLE_SERDE,
        ORC_SERDE
    }

    /**
     * Constructs a HiveInputFormatFragmenter.
     *
     * @param inputData all input parameters coming from the client
     */
    public HiveInputFormatFragmenter(InputData inputData) {
        super(inputData, HiveInputFormatFragmenter.class);
    }

    /**
     * Extracts the user data:
     * serde, partition keys and whether filter was included in fragmenter
     *
     * @param input input data from client
     * @param supportedSerdes supported serde names
     * @return parsed tokens
     * @throws UserDataException if user data contains unsupported serde
     *                           or wrong number of tokens
     */
    static public String[] parseToks(InputData input, String... supportedSerdes)
            throws UserDataException {
        String userData = new String(input.getFragmentUserData());
        String[] toks = userData.split(HIVE_UD_DELIM);
        if (supportedSerdes.length > 0
                && !Arrays.asList(supportedSerdes).contains(toks[TOK_SERDE])) {
            throw new UserDataException(toks[TOK_SERDE]
                    + " serializer isn't supported by " + input.getAccessor());
        }

        if (toks.length != (EXPECTED_NUM_OF_TOKS)) {
            throw new UserDataException("HiveInputFormatFragmenter expected "
                    + EXPECTED_NUM_OF_TOKS + " tokens, but got " + toks.length);
        }

        return toks;
    }

    /*
     * Checks that hive fields and partitions match the HAWQ schema. Throws an
     * exception if: - the number of fields (+ partitions) do not match the HAWQ
     * table definition. - the hive fields types do not match the HAWQ fields.
     */
    @Override
    void verifySchema(Table tbl) throws Exception {

        int columnsSize = inputData.getColumns();
        int hiveColumnsSize = tbl.getSd().getColsSize();
        int hivePartitionsSize = tbl.getPartitionKeysSize();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Hive table: " + hiveColumnsSize + " fields, "
                    + hivePartitionsSize + " partitions. " + "HAWQ table: "
                    + columnsSize + " fields.");
        }

        // check schema size
        if (columnsSize != (hiveColumnsSize + hivePartitionsSize)) {
            throw new IllegalArgumentException("Hive table schema ("
                    + hiveColumnsSize + " fields, " + hivePartitionsSize
                    + " partitions) " + "doesn't match PXF table ("
                    + columnsSize + " fields)");
        }

        int index = 0;
        // check hive fields
        List<FieldSchema> hiveColumns = tbl.getSd().getCols();
        for (FieldSchema hiveCol : hiveColumns) {
            ColumnDescriptor colDesc = inputData.getColumn(index++);
            DataType colType = DataType.get(colDesc.columnTypeCode());
            HiveUtilities.validateTypeCompatible(colType, colDesc.columnTypeModifiers(), hiveCol.getType(), colDesc.columnName());
        }
        // check partition fields
        List<FieldSchema> hivePartitions = tbl.getPartitionKeys();
        for (FieldSchema hivePart : hivePartitions) {
            ColumnDescriptor colDesc = inputData.getColumn(index++);
            DataType colType = DataType.get(colDesc.columnTypeCode());
            HiveUtilities.validateTypeCompatible(colType, colDesc.columnTypeModifiers(), hivePart.getType(), colDesc.columnName());
        }

    }

    /**
     * Returns statistics for Hive table. Currently it's not implemented.
     */
    @Override
    public FragmentsStats getFragmentsStats() throws Exception {
        throw new UnsupportedOperationException("ANALYZE for HiveRc, HiveText, and HiveOrc plugins is not supported");
    }
}
