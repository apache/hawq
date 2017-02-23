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

package org.apache.hawq.pxf.plugins.hive;

/**
 * Class which is a carrier for user data in Hive fragment.
 *
 */
public class HiveUserData {

    public static final String HIVE_UD_DELIM = "!HUDD!";
    private static final int EXPECTED_NUM_OF_TOKS = 7;

    public HiveUserData(String inputFormatName, String serdeClassName,
            String propertiesString, String partitionKeys,
            boolean filterInFragmenter,
            String delimiter,
            String colTypes) {

        this.inputFormatName = inputFormatName;
        this.serdeClassName = serdeClassName;
        this.propertiesString = propertiesString;
        this.partitionKeys = partitionKeys;
        this.filterInFragmenter = filterInFragmenter;
        this.delimiter = (delimiter == null ? "0" : delimiter);
        this.colTypes = colTypes;
    }

    /**
     * Returns input format of a fragment
     *
     * @return input format of a fragment
     */
    public String getInputFormatName() {
        return inputFormatName;
    }

    /**
     * Returns SerDe class name
     *
     * @return SerDe class name
     */
    public String getSerdeClassName() {
        return serdeClassName;
    }

    /**
     * Returns properties string needed for SerDe initialization
     *
     * @return properties string needed for SerDe initialization
     */
    public String getPropertiesString() {
        return propertiesString;
    }

    /**
     * Returns partition keys
     *
     * @return partition keys
     */
    public String getPartitionKeys() {
        return partitionKeys;
    }

    /**
     * Returns whether filtering was done in fragmenter
     *
     * @return true if filtering was done in fragmenter
     */
    public boolean isFilterInFragmenter() {
        return filterInFragmenter;
    }

    /**
     * Returns field delimiter
     *
     * @return field delimiter
     */
    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    private String inputFormatName;
    private String serdeClassName;
    private String propertiesString;
    private String partitionKeys;
    private boolean filterInFragmenter;
    private String delimiter;
    private String colTypes;

    /**
     * The method returns expected number of tokens in raw user data
     *
     * @return number of tokens in raw user data
     */
    public static int getNumOfTokens() {
        return EXPECTED_NUM_OF_TOKS;
    }

    @Override
    public String toString() {
        return inputFormatName + HiveUserData.HIVE_UD_DELIM
                + serdeClassName + HiveUserData.HIVE_UD_DELIM
                + propertiesString + HiveUserData.HIVE_UD_DELIM
                + partitionKeys + HiveUserData.HIVE_UD_DELIM
                + filterInFragmenter + HiveUserData.HIVE_UD_DELIM
                + delimiter + HiveUserData.HIVE_UD_DELIM
                + colTypes;
    }

    public String getColTypes() {
        return colTypes;
    }
}
