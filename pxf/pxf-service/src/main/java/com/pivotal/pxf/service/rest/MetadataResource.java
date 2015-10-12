package com.pivotal.pxf.service.rest;

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

import com.pivotal.pxf.api.Metadata;
import com.pivotal.pxf.api.MetadataFetcher;
import com.pivotal.pxf.service.MetadataFetcherFactory;
import com.pivotal.pxf.service.MetadataResponseFormatter;

/**
 * Class enhances the API of the WEBHDFS REST server.
 * Returns the metadata of a given hcatalog table.
 * <br>
 * Example for querying API FRAGMENTER from a web client:<br>
 * <code>curl -i "http://localhost:51200/pxf/v13/Metadata/getTableMetadata?table=t1"</code><br>
 * /pxf/ is made part of the path when there is a webapp by that name in tomcat.
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Metadata/")
public class MetadataResource extends RestResource {
    private Log Log;

    public MetadataResource() throws IOException {
        Log = LogFactory.getLog(MetadataResource.class);
    }

    /**
     * This function queries the HiveMetaStore to get the given table's metadata:
     * Table name, field names, field types.
     * The types are converted from HCatalog types to HAWQ types.
     * Supported HCatalog types:
     * TINYINT, SMALLINT, INT, BIGINT, BOOLEAN, FLOAT, DOUBLE,
     * STRING, BINARY, TIMESTAMP, DATE, DECIMAL, VARCHAR, CHAR.
     * <br>
     * Unsupported types result in an error.
     * <br>
     * Response Examples:<br>
     * For a table <code>default.t1</code> with 2 fields (a int, b float) will be returned as:
     *      <code>{"PXFMetadata":[{"table":{"dbName":"default","tableName":"t1"},"fields":[{"name":"a","type":"int"},{"name":"b","type":"float"}]}]}</code>
     *
     * @param servletContext servlet context
     * @param headers http headers
     * @param table HCatalog table name
     * @return JSON formatted response with metadata for given table
     * @throws Exception if connection to Hcatalog failed, table didn't exist or its type or fields are not supported
     */
    @GET
    @Path("getTableMetadata")
    @Produces("application/json")
    public Response read(@Context final ServletContext servletContext,
            			 @Context final HttpHeaders headers,
            			 @QueryParam("table") final String table) throws Exception {
        Log.debug("getTableMetadata started");
        String jsonOutput;
        try {
        	// 1. start MetadataFetcher
        	MetadataFetcher metadataFetcher =
        	        MetadataFetcherFactory.create("com.pivotal.pxf.plugins.hive.HiveMetadataFetcher"); //TODO: nhorn - 09-03-15 - pass as param

        	// 2. get Metadata
        	Metadata metadata = metadataFetcher.getTableMetadata(table);

        	// 3. serialize to JSON
        	jsonOutput = MetadataResponseFormatter.formatResponseString(metadata);

            Log.debug("getTableMetadata output: " + jsonOutput);

        } catch (ClientAbortException e) {
            Log.error("Remote connection closed by HAWQ", e);
            throw e;
        } catch (java.io.IOException e) {
            Log.error("Unhandled exception thrown", e);
            throw e;
        }

        return Response.ok(jsonOutput, MediaType.APPLICATION_JSON_TYPE).build();
    }
}
