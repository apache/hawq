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

public class HiveUserData {

    public static final String HIVE_UD_DELIM = "!HUDD!";

    public HiveUserData(String inputFormatName, String serdeClassName,
            String propertiesString, String partitionKeys,
            boolean filterInFragmenter,
            String collectionDelim,
            String mapKeyDelim,
            String delimiter) {

        this.inputFormatName = inputFormatName;
        this.serdeClassName = serdeClassName;
        this.propertiesString = propertiesString;
        this.partitionKeys = partitionKeys;
        this.filterInFragmenter = filterInFragmenter;
        this.collectionDelim = (collectionDelim == null ? "0" : collectionDelim);
        this.mapKeyDelim = (mapKeyDelim == null ? "0" : mapKeyDelim);
        this.delimiter = (delimiter == null ? "0" : delimiter);
    }

    public String getInputFormatName() {
        return inputFormatName;
    }

    public String getSerdeClassName() {
        return serdeClassName;
    }

    public String getPropertiesString() {
        return propertiesString;
    }

    public String getPartitionKeys() {
        return partitionKeys;
    }

    public boolean isFilterInFragmenter() {
        return filterInFragmenter;
    }

    public String getCollectionDelim() {
        return collectionDelim;
    }

    public void setCollectionDelim(String collectionDelim) {
        this.collectionDelim = collectionDelim;
    }

    public String getMapKeyDelim() {
        return mapKeyDelim;
    }

    public void setMapKeyDelim(String mapKeyDelim) {
        this.mapKeyDelim = mapKeyDelim;
    }

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
    private String collectionDelim;
    private String mapKeyDelim;
    private String delimiter;

    @Override
    public String toString() {
        return inputFormatName + HiveUserData.HIVE_UD_DELIM
                + serdeClassName + HiveUserData.HIVE_UD_DELIM
                + propertiesString + HiveUserData.HIVE_UD_DELIM
                + partitionKeys + HiveUserData.HIVE_UD_DELIM
                + filterInFragmenter + HiveUserData.HIVE_UD_DELIM
                + collectionDelim + HiveUserData.HIVE_UD_DELIM
                + mapKeyDelim + HiveUserData.HIVE_UD_DELIM
                + delimiter;
    }
}
