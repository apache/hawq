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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

import org.apache.hawq.pxf.api.Metadata;

/**
 * Utility class for converting {@link Metadata} into a JSON format.
 */
public class MetadataResponseFormatter {

    private static final Log LOG = LogFactory.getLog(MetadataResponseFormatter.class);
    private static final String METADATA_DEFAULT_RESPONSE = "{\"PXFMetadata\":[]}";

    /**
     * Converts list of {@link Metadata} to JSON String format.
     *
     * @param metadataList list of metadata objects to convert
     * @return JSON formatted response
     * @throws IOException if converting the data to JSON fails
     */
    public static String formatResponseString(List<Metadata> metadataList) throws IOException {
        return MetadataResponseFormatter.metadataToJSON(metadataList);
    }

    /**
     * Serializes a metadata in JSON,
     * To be used as the result string for HAWQ.
     * An example result is as follows:
     *
     * {"PXFMetadata":[{"item":{"path":"default","name":"t1"},"fields":[{"name":"a","type":"int"},{"name":"b","type":"float"}]}]}
     */
    private static String metadataToJSON(List<Metadata> metadataList) throws IOException {

        if (metadataList == null || metadataList.isEmpty()) {
               return METADATA_DEFAULT_RESPONSE;
        }

        StringBuilder result = null;

        for(Metadata metadata: metadataList) {
            if(metadata == null) {
                throw new IllegalArgumentException("metadata object is null - cannot serialize");
            }
            if ((metadata.getFields() == null) || metadata.getFields().isEmpty()) {
                throw new IllegalArgumentException("metadata for " + metadata.getItem() + " contains no fields - cannot serialize");
            }
            if (result == null) {
                result = new StringBuilder("{\"PXFMetadata\":["); /* prefix info */
            } else {
                result.append(",");
            }

            ObjectMapper mapper = new ObjectMapper();
            mapper.setSerializationInclusion(Inclusion.NON_EMPTY); // ignore empty fields
            result.append(mapper.writeValueAsString(metadata));
        }

        return result.append("]}").toString(); /* append suffix info */

    }

    /**
     * Converts metadata list to a readable string.
     * Intended for debugging purposes only.
     */
    private static String metadataToString(List<Metadata> metadataList) {
        StringBuilder result = new StringBuilder("Metadata:");

        for(Metadata metadata: metadataList) {
            result.append(" Metadata for item \"");

            if (metadata == null) {
                return "No metadata";
            }

            result.append(metadata.getItem()).append("\": ");

            if ((metadata.getFields() == null) || metadata.getFields().isEmpty()) {
                result.append("no fields in item");
                return result.toString();
            }

            int i = 0;
            for (Metadata.Field field : metadata.getFields()) {
                result.append("Field #").append(++i).append(": [")
                        .append("Name: ").append(field.getName())
                        .append(", Type: ").append(field.getType()).append("] ");
            }
        }

        return result.toString();
    }
}
