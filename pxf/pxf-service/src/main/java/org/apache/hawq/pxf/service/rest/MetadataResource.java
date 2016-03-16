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
import org.apache.hawq.pxf.service.MetadataFetcherFactory;
import org.apache.hawq.pxf.service.MetadataResponseFormatter;

/**
 * Class enhances the API of the WEBHDFS REST server. Returns the metadata of a
 * given hcatalog table. <br>
 * Example for querying API FRAGMENTER from a web client:<br>
 * <code>curl -i "http://localhost:51200/pxf/{version}/Metadata/getTableMetadata?table=t1"</code>
 * <br>
 * /pxf/ is made part of the path when there is a webapp by that name in tomcat.
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Metadata/")
public class MetadataResource extends RestResource {
    private static final Log LOG = LogFactory.getLog(MetadataResource.class);

    public MetadataResource() throws IOException {
    }

    /**
     * This function queries the underlying store based on the given profile to get schema for the given item(s)
     * metadata: Item name, field names, field types. The types are converted
     * from the underlying types to HAWQ types.
     * Unsupported types result in an error. <br>
     * Response Examples:<br>
     * For a table <code>default.t1</code> with 2 fields (a int, b float) will
     * be returned as:
     * <code>{"PXFMetadata":[{"item":{"path":"default","name":"t1"},"fields":[{"name":"a","type":"int"},{"name":"b","type":"float"}]}]}</code>
     *
     * @param servletContext servlet context
     * @param headers http headers
     * @param profile based on this the metadata source can be inferred
     * @param item table/file name or pattern in the given source
     * @return JSON formatted response with metadata for given item(s)
     * @throws Exception if connection to the source/catalog failed, item didn't exist or
     *             its type or fields are not supported
     */
    @GET
    @Path("getMetadata")
    @Produces("application/json")
    public Response read(@Context final ServletContext servletContext,
                         @Context final HttpHeaders headers,
                         @QueryParam("profile") final String profile,
                         @QueryParam("item") final String item)
            throws Exception {
        LOG.debug("getMetadata started");
        String jsonOutput;
        try {
            // 1. start MetadataFetcher
            MetadataFetcher metadataFetcher = MetadataFetcherFactory.create(profile.toLowerCase());

            // 2. get Metadata
            List<Metadata> metadata = metadataFetcher.getMetadata(item);

            // 3. serialize to JSON
            jsonOutput = MetadataResponseFormatter.formatResponseString(metadata);

            LOG.debug("getMetadata output: " + jsonOutput);

        } catch (ClientAbortException e) {
            LOG.error("Remote connection closed by HAWQ", e);
            throw e;
        } catch (java.io.IOException e) {
            LOG.error("Unhandled exception thrown", e);
            throw e;
        }

        return Response.ok(jsonOutput, MediaType.APPLICATION_JSON_TYPE).build();
    }
}
