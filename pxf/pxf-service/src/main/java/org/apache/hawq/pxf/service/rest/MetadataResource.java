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
 * <code>curl -i "http://localhost:51200/pxf/v13/Metadata/getTableMetadata?table=t1"</code>
 * <br>
 * /pxf/ is made part of the path when there is a webapp by that name in tomcat.
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Metadata/")
public class MetadataResource extends RestResource {
    private static final Log LOG = LogFactory.getLog(MetadataResource.class);

    public MetadataResource() throws IOException {
    }

    /**
     * This function queries the HiveMetaStore to get the given table's
     * metadata: Table name, field names, field types. The types are converted
     * from HCatalog types to HAWQ types. Supported HCatalog types: TINYINT,
     * SMALLINT, INT, BIGINT, BOOLEAN, FLOAT, DOUBLE, STRING, BINARY, TIMESTAMP,
     * DATE, DECIMAL, VARCHAR, CHAR. <br>
     * Unsupported types result in an error. <br>
     * Response Examples:<br>
     * For a table <code>default.t1</code> with 2 fields (a int, b float) will
     * be returned as:
     * <code>{"PXFMetadata":[{"table":{"dbName":"default","tableName":"t1"},"fields":[{"name":"a","type":"int"},{"name":"b","type":"float"}]}]}</code>
     *
     * @param servletContext servlet context
     * @param headers http headers
     * @param table HCatalog table name
     * @return JSON formatted response with metadata for given table
     * @throws Exception if connection to Hcatalog failed, table didn't exist or
     *             its type or fields are not supported
     */
    @GET
    @Path("getTableMetadata")
    @Produces("application/json")
    public Response read(@Context final ServletContext servletContext,
                         @Context final HttpHeaders headers,
                         @QueryParam("table") final String table)
            throws Exception {
        LOG.debug("getTableMetadata started");
        String jsonOutput;
        try {
            // 1. start MetadataFetcher
            MetadataFetcher metadataFetcher = MetadataFetcherFactory.create("org.apache.hawq.pxf.plugins.hive.HiveMetadataFetcher"); // TODO:
                                                                                                                                     // nhorn
                                                                                                                                     // -
                                                                                                                                     // 09-03-15
                                                                                                                                     // -
                                                                                                                                     // pass
                                                                                                                                     // as
                                                                                                                                     // param

            // 2. get Metadata
            Metadata metadata = metadataFetcher.getTableMetadata(table);

            // 3. serialize to JSON
            jsonOutput = MetadataResponseFormatter.formatResponseString(metadata);

            LOG.debug("getTableMetadata output: " + jsonOutput);

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
