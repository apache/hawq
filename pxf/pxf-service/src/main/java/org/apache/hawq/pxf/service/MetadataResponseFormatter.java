package org.apache.hawq.pxf.service;

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


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

import org.apache.hawq.pxf.api.Metadata;

/**
 * Utility class for converting {@link Metadata} into a JSON format.
 */
public class MetadataResponseFormatter {

    private static Log Log = LogFactory.getLog(MetadataResponseFormatter.class);

    /**
     * Converts {@link Metadata} to JSON String format.
     *
     * @param metadata metadata to convert
     * @return JSON formatted response
     * @throws IOException if converting the data to JSON fails
     */
    public static String formatResponseString(Metadata metadata) throws IOException {
        /* print the metadata before serialization */
        Log.debug(MetadataResponseFormatter.metadataToString(metadata));

        return MetadataResponseFormatter.metadataToJSON(metadata);
    }

    /**
     * Serializes a metadata in JSON,
     * To be used as the result string for HAWQ.
     * An example result is as follows:
     *
     * {"PXFMetadata":[{"table":{"dbName":"default","tableName":"t1"},"fields":[{"name":"a","type":"int"},{"name":"b","type":"float"}]}]}
     */
    private static String metadataToJSON(Metadata metadata) throws IOException {

        if (metadata == null) {
            throw new IllegalArgumentException("metadata object is null - cannot serialize");
        }

        if ((metadata.getFields() == null) || metadata.getFields().isEmpty()) {
            throw new IllegalArgumentException("metadata contains no fields - cannot serialize");
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(Inclusion.NON_EMPTY); // ignore empty fields

        StringBuilder result = new StringBuilder("{\"PXFMetadata\":");
        String prefix = "["; // preparation for supporting multiple tables
        result.append(prefix).append(mapper.writeValueAsString(metadata));
        return result.append("]}").toString();
    }

    /**
     * Converts metadata to a readable string.
     * Intended for debugging purposes only.
     */
    private static String metadataToString(Metadata metadata) {
        StringBuilder result = new StringBuilder("Metadata for table \"");

        if (metadata == null) {
            return "No metadata";
        }

        result.append(metadata.getTable()).append("\": ");

        if ((metadata.getFields() == null) || metadata.getFields().isEmpty()) {
            result.append("no fields in table");
            return result.toString();
        }

        int i = 0;
        for (Metadata.Field field: metadata.getFields()) {
            result.append("Field #").append(++i).append(": [")
                .append("Name: ").append(field.getName())
                .append(", Type: ").append(field.getType()).append("] ");
        }

        return result.toString();
    }
}
