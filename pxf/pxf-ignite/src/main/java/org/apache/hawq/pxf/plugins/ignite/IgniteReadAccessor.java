package org.apache.hawq.pxf.plugins.ignite;

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

import org.apache.hawq.pxf.api.OneRow;
import org.apache.hawq.pxf.api.ReadAccessor;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.net.URL;
import java.net.URLEncoder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.ConnectException;
import java.net.ProtocolException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonArray;


/**
 * Ignite tables read accessor
 */
public class IgniteReadAccessor extends IgnitePlugin implements ReadAccessor {
    private static final Log LOG = LogFactory.getLog(IgniteReadAccessor.class);

    // Prepared queries to send to Ignite
    private String queryStart = null;
    private String queryFetch = null;
    private String queryClose = null;
    // Set to true when Ignite reported there were no data left for this query
    private boolean lastQueryFinished = false;
    // A buffer to store the query results (without Ignite metadata)
    private LinkedList<JsonArray> buffer = new LinkedList<JsonArray>();

    public IgniteReadAccessor(InputData inputData) throws Exception {
        super(inputData);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Constructor started");
        }

        StringBuilder sb = new StringBuilder();

        // Insert a list of fields to be selected
        ArrayList<ColumnDescriptor> columns = inputData.getTupleDescription();
        if (columns == null) {
            throw new UserDataException("Tuple description must be present.");
        }
        sb.append("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            ColumnDescriptor column = columns.get(i);
            if (i > 0) {
                sb.append(",");
            }
            sb.append(column.columnName());
        }

        // Insert the name of the table to select values from
        String tableName = inputData.getDataSource();
        if (tableName == null) {
            throw new UserDataException("Table name must be set as DataSource.");
        }
        sb.append(" FROM ").append(tableName);

        // Insert constraints
        // TODO:
        // Filter constants must be provided separately from the query itself, for optimizations on Ignite side.
        ArrayList<String> filterConstants = null;
        if (inputData.hasFilter()) {
            WhereSQLBuilder filterBuilder = new WhereSQLBuilder(inputData);
            String whereSql = filterBuilder.buildWhereSQL();

            if (whereSql != null) {
                sb.append(" WHERE ").append(whereSql);
            }
        }

        sb = new IgnitePartitionFragmenter(inputData).buildFragmenterSql(sb);

        queryStart = buildQueryStart(sb.toString(), filterConstants);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Constructor successful");
        }
    }

    /**
     * openForRead() implementation
     */
    @Override
    public boolean openForRead() throws Exception {
        // We do not catch exception here as it should be passed up anyway
        JsonElement response = sendRestRequest(queryStart);

        lastQueryFinished = response.getAsJsonObject().get("last").getAsBoolean();

        queryFetch = buildQueryFetch(response.getAsJsonObject().get("queryId").getAsInt());
        queryClose = buildQueryClose(response.getAsJsonObject().get("queryId").getAsInt());

        LOG.info("Performing Ignite read request. URL: '" + queryStart + "'");
        return true;
    }

    /**
     * readNextObject() implementation
     */
    @Override
    public OneRow readNextObject() throws Exception {
        if (queryFetch == null) {
            LOG.error("readNextObject(): queryFetch is null. This means the Ignite qryfldexe query was not executed properly");
            return null;
        }

        if (buffer.isEmpty()) {
            // Refill buffer
            if (lastQueryFinished) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("readNextObject(): Last query finished");
                }
                return null;
            }

            // We do not catch exception here as it should be passed up anyway
            JsonElement response = sendRestRequest(queryFetch);

            lastQueryFinished = response.getAsJsonObject().get("last").getAsBoolean();

            // Parse 'items'
            Iterator<JsonElement> itemsIterator = response.getAsJsonObject().get("items").getAsJsonArray().iterator();
            while (itemsIterator.hasNext()) {
                buffer.add(itemsIterator.next().getAsJsonArray());
            }

            // Check again in case "response" contains no elements
            if (buffer.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("readNextObject(): Buffer refill failed");
                    LOG.debug("readNextObject(): Last query finished");
                }
                return null;
            }
        }

        return new OneRow(null, buffer.pollFirst());
    }

    /**
     * closeForRead() implementation
     */
    @Override
    public void closeForRead() {
        if (queryClose != null) {
            JsonElement response;
            try {
                response = sendRestRequest(queryClose);
            }
            catch (Exception e) {
                LOG.error("closeForRead() Exception: " + e.getClass().getSimpleName());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ignite read request CLOSED. URL: '" + queryStart + "'");
        }
    }

    /**
     * Build HTTP GET query for Ignite REST API with command 'qryfldexe'
     * 
     * @param querySql SQL query with filter constants replaced by '?' 
     * @param filterConstants
     *
     * @return Prepared HTTP query properly encoded with {@link java.net.URLEncoder}
     * 
     * @throws UnsupportedEncodingException From java.net.URLEncoder.encode()
     */
    private String buildQueryStart(String querySql, ArrayList<String> filterConstants) throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        sb.append("http://");
        sb.append(igniteHost);
        sb.append("/ignite");
        sb.append("?");
        sb.append("cmd=qryfldexe");
        sb.append("&");
        sb.append("pageSize=");
        sb.append(0);
        sb.append("&");
        if (cacheName != null) {
            sb.append("cacheName=");
            // Note that Ignite supports only "good" cache names (those that should be left unchanged by the URLEncoder.encode())
            sb.append(URLEncoder.encode(cacheName, "UTF-8"));
            sb.append("&");
        }
        int counter = 1;
        if (filterConstants != null) {
            for (String constant : filterConstants) {
                sb.append("arg");
                sb.append(counter);
                sb.append("=");
                sb.append(URLEncoder.encode(constant, "UTF-8"));
                sb.append("&");
                counter += 1;
            }
        }
        sb.append("qry=");
        sb.append(URLEncoder.encode(querySql, "UTF-8"));

        return sb.toString();
    }

    /**
     * Build HTTP GET query for Ignite REST API with command 'qryfetch'
     * This query is used to retrieve data after the 'qryfldexe' command started
     * 
     * @param queryId ID of the query assigned by Ignite when the query started
     *
     * @return Prepared HTTP query
     */
    private String buildQueryFetch(int queryId) {
        StringBuilder sb = new StringBuilder();
        sb.append("http://");
        sb.append(igniteHost);
        sb.append("/ignite");
        sb.append("?");
        sb.append("cmd=qryfetch");
        sb.append("&");
        sb.append("pageSize=");
        sb.append(bufferSize);
        sb.append("&");
        sb.append("qryId=");
        sb.append(queryId);

        return sb.toString();
    }

    /**
     * Build HTTP GET query for Ignite REST API with command 'qrycls'
     * This query is used to close query resources on Ignite side
     * 
     * @param queryId ID of the query assigned by Ignite when the query started
     *
     * @return Prepared HTTP query
     */
    private String buildQueryClose(int queryId) {
        StringBuilder sb = new StringBuilder();
        sb.append("http://");
        sb.append(igniteHost);
        sb.append("/ignite");
        sb.append("?");
        sb.append("cmd=qrycls");
        sb.append("&");
        sb.append("qryId=");
        sb.append(queryId);

        return sb.toString();
    }

    /**
     * Send a REST request to the Ignite server
     * 
     * @param query A prepared and properly encoded HTTP GET request
     * 
     * @return "response" field from the received JSON object
     * (See Ignite REST API documentation for details)
     * 
     * @throws ProtocolException if Ignite reports error in it's JSON response
     * @throws MalformedURLException if URL is malformed
     * @throws IOException in case of connection failure
     */
    private JsonElement sendRestRequest(String query) throws ProtocolException, MalformedURLException, IOException {
        // Create URL object
        URL url;
        try {
            url = new URL(query);
        }
        catch (MalformedURLException e) {
            LOG.error("sendRestRequest(): Failed (malformed url). URL is '" + query + "'");
            throw e;
        }

        // Connect to the Ignite server, send query and get raw response
        String responseRaw = null;
        try {
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
            String responseLine;
            while ((responseLine = reader.readLine()) != null) {
                sb.append(responseLine);
            }
            reader.close();
            responseRaw = sb.toString();
            if (LOG.isDebugEnabled()) {
                LOG.debug("sendRestRequest(): URL: '" + query + "'; Result: '" + responseRaw + "'");
            }
        }
        catch (Exception e) {
            LOG.error("sendRestRequest(): Failed (connection failure). URL is '" + query + "'");
            throw e;
        }
        
        // Parse raw Ignite server response
        JsonElement response = null;
        String error = null;
        int successStatus;
        try {
            response = new JsonParser().parse(responseRaw);

            if (!response.getAsJsonObject().get("error").isJsonNull()) {
                error = response.getAsJsonObject().get("error").getAsString();
            }
            successStatus = response.getAsJsonObject().get("successStatus").getAsInt();
        }
        catch (Exception e) {
            LOG.error("sendRestRequest(): Failed (JSON parsing failure). URL is '" + query + "'");
            throw e;
        }

        // Check errors reported by Ignite
        if ((error != null) || (successStatus != 0)) {
            LOG.error("sendRestRequest(): Failed (failure on Ignite side: '" + error + "'). URL is '" + query + "'");
            throw new ProtocolException("Ignite failure: status " + successStatus + ", '" + error + "'");
        }

        // Return response without metadata
        try {
            return response.getAsJsonObject().get("response");
        }
        catch (Exception e) {
            LOG.error("sendRestRequest(): Failed (JSON parsing failure). URL is '" + query + "'");
            throw e;
        }
    }
}
