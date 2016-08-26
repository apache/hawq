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

    /**
     * Converts list of {@link Metadata} to JSON String format.
     *
     * @param metadataList list of metadata objects to convert
     * @param path path string
     * @return JSON formatted response
     * @throws IOException if converting the data to JSON fails
     */
    public static MetadataResponse formatResponse(List<Metadata> metadataList, String path) throws IOException {
        /* print the fragment list to log when in debug level */
        if (LOG.isDebugEnabled()) {
            MetadataResponseFormatter.printMetadata(metadataList, path);
        }

        return new MetadataResponse(metadataList);
    }

    /**
     * Converts metadata list to a readable string.
     * Intended for debugging purposes only.
     */
    private static void printMetadata(List<Metadata> metadataList, String path) {
        LOG.debug("Metadata List for path " + path + ": ");

        if (null == metadataList || metadataList.isEmpty()) {
            LOG.debug("No metadata");
            return;
        }

        for(Metadata metadata: metadataList) {
            StringBuilder result = new StringBuilder();

            if (metadata == null) {
                result.append("None");
                LOG.debug(result);
                continue;
            }

            result.append("Metadata for item \"").append(metadata.getItem()).append("\": ");

            if ((metadata.getFields() == null) || metadata.getFields().isEmpty()) {
                result.append("None");
            } else {
                int i = 0;
                for (Metadata.Field field : metadata.getFields()) {
                    result.append("Field #").append(++i).append(": [")
                            .append("Name: ").append(field.getName())
                            .append(", Type: ").append(field.getType().getTypeName())
                            .append(", Source type: ").append(field.getSourceType()).append("] ");
                }
            }
            LOG.debug(result);
        }
    }
}
