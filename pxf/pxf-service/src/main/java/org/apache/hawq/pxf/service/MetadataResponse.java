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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.api.Metadata;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;


/**
 * Class for serializing metadata in JSON format. The class implements
 * {@link StreamingOutput} so the serialization will be done in a stream and not
 * in one bulk, this in order to avoid running out of memory when processing a
 * lot of items.
 */
public class MetadataResponse implements StreamingOutput {

    private static final Log Log = LogFactory.getLog(MetadataResponse.class);
    private static final String METADATA_DEFAULT_RESPONSE = "{\"PXFMetadata\":[]}";

    private List<Metadata> metadataList;

    /**
     * Constructs metadata response out of a metadata list
     *
     * @param metadataList metadata list
     */
    public MetadataResponse(List<Metadata> metadataList) {
        this.metadataList = metadataList;
    }

    /**
     * Serializes the metadata list in JSON, To be used as the result string for HAWQ.
     */
    @Override
    public void write(OutputStream output) throws IOException,
            WebApplicationException {
        DataOutputStream dos = new DataOutputStream(output);
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(Inclusion.NON_EMPTY); // ignore empty fields

        if(metadataList == null || metadataList.isEmpty()) {
            dos.write(METADATA_DEFAULT_RESPONSE.getBytes());
            return;
        }

        dos.write("{\"PXFMetadata\":[".getBytes());

        String prefix = "";
        for (Metadata metadata : metadataList) {
            if(metadata == null) {
                throw new IllegalArgumentException("metadata object is null - cannot serialize");
            }
            if ((metadata.getFields() == null) || metadata.getFields().isEmpty()) {
                throw new IllegalArgumentException("metadata for " + metadata.getItem() + " contains no fields - cannot serialize");
            }
            StringBuilder result = new StringBuilder();
            result.append(prefix).append(mapper.writeValueAsString(metadata));
            prefix = ",";
            dos.write(result.toString().getBytes());
        }

        dos.write("]}".getBytes());
    }
}
