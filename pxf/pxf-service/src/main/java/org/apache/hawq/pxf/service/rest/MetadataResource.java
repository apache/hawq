package org.apache.hawq.pxf.service.rest;

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
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hawq.pxf.api.Metadata;
import org.apache.hawq.pxf.api.MetadataFetcher;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.service.MetadataFetcherFactory;
import org.apache.hawq.pxf.service.MetadataResponse;
import org.apache.hawq.pxf.service.MetadataResponseFormatter;
import org.apache.hawq.pxf.service.utilities.ProtocolData;
import org.apache.hawq.pxf.service.utilities.SecuredHDFS;

/**
 * Class enhances the API of the WEBHDFS REST server. Returns the metadata of a
 * given hcatalog table. <br>
 * Example for querying API FRAGMENTER from a web client:<br>
 * <code>curl -i "http://localhost:51200/pxf/{version}/Metadata/getMetadata?profile=PROFILE_NAME&amp;pattern=OBJECT_PATTERN"</code>
 * <br>
 * /pxf/ is made part of the path when there is a webapp by that name in tomcat.
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Metadata/")
public class MetadataResource extends RestResource {
    private static final Log LOG = LogFactory.getLog(MetadataResource.class);

    public MetadataResource() throws IOException {
    }

    /**
     * This function queries the underlying store based on the given profile to get schema for items that match the given pattern
     * metadata: Item name, field names, field types. The types are converted
     * from the underlying types to HAWQ types.
     * Unsupported types result in an error. <br>
     * Response Examples:<br>
     * For a table <code>default.t1</code> with 2 fields (a int, b float) will
     * be returned as:
     * <code>{"PXFMetadata":[{"item":{"path":"default","name":"t1"},
     * "fields":[{"name":"a","type":"int4","sourceType":"int","complexType":false},
     * {"name":"b","type":"float8","sourceType":"double","complexType":false}],
     * "outputFormats":["TEXT"],
     * "outputParameters":{"DELIMITER":"1"}}]}
     * </code>
     *
     * @param servletContext servlet context
     * @param headers http headers
     * @param profile based on this the metadata source can be inferred
     * @param pattern table/file name or pattern in the given source
     * @return JSON formatted response with metadata of each item that corresponds to the pattern
     * @throws Exception if connection to the source/catalog failed, item didn't exist for the pattern
     *             its type or fields are not supported
     */
    @GET
    @Path("getMetadata")
    @Produces("application/json")
    public Response read(@Context final ServletContext servletContext,
                         @Context final HttpHeaders headers,
                         @QueryParam("profile") final String profile,
                         @QueryParam("pattern") final String pattern)
            throws Exception {
        LOG.debug("getMetadata started");
        String jsonOutput;
        try {

            // Convert headers into a regular map
            Map<String, String> params = convertToCaseInsensitiveMap(headers.getRequestHeaders());

            // Add profile
            ProtocolData protData = new ProtocolData(params, profile.toLowerCase());

            // Token verification happens at the SecurityServletFilter

            // 1. start MetadataFetcher
            MetadataFetcher metadataFetcher = MetadataFetcherFactory.create(protData);

            // 2. get Metadata
            List<Metadata> metadata = metadataFetcher.getMetadata(pattern);

            // 3. stream JSON ouptput
            MetadataResponse metadataResponse = MetadataResponseFormatter.formatResponse(
                    metadata, pattern);

            return Response.ok(metadataResponse, MediaType.APPLICATION_JSON_TYPE).build();

        } catch (ClientAbortException e) {
            LOG.error("Remote connection closed by HAWQ", e);
            throw e;
        } catch (java.io.IOException e) {
            LOG.error("Unhandled exception thrown", e);
            throw e;
        }

    }
}
