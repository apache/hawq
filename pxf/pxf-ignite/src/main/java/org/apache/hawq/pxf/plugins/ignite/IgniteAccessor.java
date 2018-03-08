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
import org.apache.hawq.pxf.api.WriteAccessor;
import org.apache.hawq.pxf.api.UserDataException;
import org.apache.hawq.pxf.api.utilities.ColumnDescriptor;
import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.ignite.IgnitePlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.MalformedURLException;
import java.net.ProtocolException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonArray;

/**
 * PXF-Ignite accessor class
 */
public class IgniteAccessor extends IgnitePlugin implements ReadAccessor, WriteAccessor {
    /**
     * Class constructor
     */
    public IgniteAccessor(InputData inputData) throws UserDataException {
        super(inputData);
    }

    /**
     * openForRead() implementation
     */
    @Override
    public boolean openForRead() throws Exception {
        if (bufferSize == 0) {
            bufferSize = 1;
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
                sb.append(", ");
            }
            sb.append(column.columnName());
        }

        // Insert the name of the table to select values from
        sb.append(" FROM ");
        String tableName = inputData.getDataSource();
        if (tableName == null) {
            throw new UserDataException("Table name must be set as DataSource.");
        }
        sb.append(tableName);

        // Insert query constraints
        // Note: Filter constants may be passed to Ignite separately from the WHERE expression, primarily for the safety of the SQL queries. However, at the moment they are passed in the query.
        ArrayList<String> filterConstants = null;
        if (inputData.hasFilter()) {
            WhereSQLBuilder filterBuilder = new WhereSQLBuilder(inputData);
            String whereSql = filterBuilder.buildWhereSQL();

            if (whereSql != null) {
                sb.append(" WHERE ").append(whereSql);
            }
        }

        // Insert partition constraints
        IgnitePartitionFragmenter.buildFragmenterSql(inputData, sb);

        // Format URL
        urlReadStart = buildQueryFldexe(sb.toString(), filterConstants);

        // Send the first REST request that opens the connection
        JsonElement response = sendRestRequest(urlReadStart);

        // Build 'urlReadFetch' and 'urlReadClose'
        isLastReadFinished = response.getAsJsonObject().get("last").getAsBoolean();
        urlReadFetch = buildQueryFetch(response.getAsJsonObject().get("queryId").getAsInt());
        urlReadClose = buildQueryCls(response.getAsJsonObject().get("queryId").getAsInt());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ignite read request. URL: '" + urlReadStart + "'");
        }
        return true;
    }

    /**
     * readNextObject() implementation
     */
    @Override
    public OneRow readNextObject() throws Exception {
        if (urlReadFetch == null) {
            LOG.error("readNextObject(): urlReadFetch is null. This means the Ignite qryfldexe query was not executed properly");
            throw new ProtocolException("readNextObject(): urlReadFetch is null. This means the Ignite qryfldexe query was not executed properly");
        }

        if (bufferRead.isEmpty()) {
            // Refill buffer
            if (isLastReadFinished) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("readNextObject(): All the data received from Ignite");
                }
                return null;
            }

            JsonElement response = sendRestRequest(urlReadFetch);
            isLastReadFinished = response.getAsJsonObject().get("last").getAsBoolean();

            // Parse 'items'
            Iterator<JsonElement> itemsIterator = response.getAsJsonObject().get("items").getAsJsonArray().iterator();
            while (itemsIterator.hasNext()) {
                if (!bufferRead.add(itemsIterator.next().getAsJsonArray())) {
                    throw new IOException("readNextObject(): not enough memory in 'bufferRead'");
                }
            }

            // Check again in case "response" contains no elements
            if (bufferRead.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("readNextObject(): Buffer refill failed");
                    LOG.debug("readNextObject(): All the data received from Ignite");
                }
                return null;
            }
        }

        return new OneRow(bufferRead.pollFirst());
    }

    /**
     * closeForRead() implementation
     */
    @Override
    public void closeForRead() {
        if (urlReadClose != null) {
            try {
                sendRestRequest(urlReadClose);
            }
            catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("closeForRead() Exception: " + e.getClass().getSimpleName());
                }
            }
        }
        isLastReadFinished = false;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ignite read request finished. URL: '" + urlReadClose + "'");
        }
    }

    /**
     * openForWrite() implementation.
     * No queries are sent to Ignite by this procedure, so if there are some problems (for example, with connection), they will be revealed only during the execution of 'writeNextObject()'
     */
    @Override
    public boolean openForWrite() throws UserDataException {
        // This is a temporary solution. At the moment there is no other way (except for the usage of user-defined parameters) to get the correct name of Ignite table: GPDB inserts extra data into the address, as required by Hadoop.
        // Note that if no extra data is present, the 'definedSource' will be left unchanged
        String definedSource = inputData.getDataSource();
        Matcher matcher = writeAddressPattern.matcher(definedSource);
        if (matcher.find()) {
            inputData.setDataSource(matcher.group(1));
        }

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");

        // Insert the table name
        String tableName = inputData.getDataSource();
        if (tableName == null) {
            throw new UserDataException("Table name must be set as DataSource.");
        }
        sb.append(tableName);

        // Insert the column names
        sb.append("(");
        ArrayList<ColumnDescriptor> columns = inputData.getTupleDescription();
        if (columns == null) {
            throw new UserDataException("Tuple description must be present.");
        }
        String fieldDivisor = "";
        for (int i = 0; i < columns.size(); i++) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            sb.append(columns.get(i).columnName());
        }
        sb.append(")");

        sb.append(" VALUES ");

        queryWrite = sb.toString();
        return true;
    }

    /**
     * writeNextObject() implementation
     */
    @Override
    public boolean writeNextObject(OneRow currentRow) throws Exception {
        boolean currentRowInBuffer = bufferWrite.add(currentRow);

        if (!isWriteActive) {
            if (!currentRowInBuffer) {
                LOG.error("writeNextObject(): Failed (not enough memory in 'bufferWrite')");
                throw new IOException("writeNextObject(): not enough memory in 'bufferWrite'");
            }
            LOG.info("Ignite write request. Query: '" + queryWrite + "'");
            sendInsertRestRequest(queryWrite);
            isWriteActive = true;
            return true;
        }

        if ((bufferWrite.size() >= bufferSize) || (!currentRowInBuffer)) {
            sendInsertRestRequest(queryWrite);
        }

        if (!currentRowInBuffer) {
            if (!bufferWrite.add(currentRow)) {
                LOG.error("writeNextObject(): Failed (not enough memory in 'bufferSend')");
                throw new IOException("writeNextObject(): not enough memory in 'bufferSend'");
            }
        }

        return true;
    }

    /**
     * closeForWrite() implementation
     */
    @Override
    public void closeForWrite() throws Exception {
        isWriteActive = false;
        if (!bufferWrite.isEmpty()) {
            sendInsertRestRequest(queryWrite);
        }
        if (isWriteActive) {
            // At this point, the request must have finished successfully
            LOG.info("Ignite write request finished successfully. Query: '" + queryWrite + "'");
        }
    }


    private static final Log LOG = LogFactory.getLog(IgniteAccessor.class);

    // A pattern to cut extra parameters from 'InputData.dataSource' when write operation is performed. See {@link openForWrite()} for the details
    private static final Pattern writeAddressPattern = Pattern.compile("/(.*)/[0-9]*-[0-9]*_[0-9]*");

    // Prepared URLs to send to Ignite when reading data
    private String urlReadStart = null;
    private String urlReadFetch = null;
    private String urlReadClose = null;
    // Set to true when Ignite reported all the data for the SELECT query was retreived
    private boolean isLastReadFinished = false;
    // A buffer to store the SELECT query results (without Ignite metadata)
    private LinkedList<JsonArray> bufferRead = new LinkedList<JsonArray>();

    // A template for the INSERT
    private String queryWrite = null;
    // Set to true when the INSERT operation is in progress
    private boolean isWriteActive = false;
    // A buffer to store prepared values for the INSERT query
    private LinkedList<OneRow> bufferWrite = new LinkedList<OneRow>();

    /**
     * Build HTTP GET query for Ignite REST API with command 'qryfldexe'
     *
     * @param querySql SQL query
     * @param filterConstants A list of Constraints' constants. Must be null in this version.
     *
     * @return Prepared HTTP query. The query will be properly encoded with {@link java.net.URLEncoder}
     *
     * @throws UnsupportedEncodingException from {@link java.net.URLEncoder.encode()}
     */
    private String buildQueryFldexe(String querySql, List<String> filterConstants) throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        sb.append("http://");
        sb.append(igniteHost);
        sb.append("/ignite");
        sb.append("?");
        sb.append("cmd=qryfldexe");
        sb.append("&");
        sb.append("pageSize=0");
        sb.append("&");
        if (cacheName != null) {
            sb.append("cacheName=");
            // Note that Ignite supports only "good" cache names (those that will be left unchanged by the URLEncoder.encode())
            sb.append(URLEncoder.encode(cacheName, "UTF-8"));
            sb.append("&");
        }
        /*
        'filterConstants' must always be null in the current version.
        This code allows to pass filters' constants separately from the filters' expressions. This feature is supported by Ignite database; however, it is not implemented in PXF Ignite plugin at the moment.
        To implement this, changes should be made in {@link WhereSQLBuilder} (form SQL query without filter constants) and {@link IgnitePartitionFragmenter} (form partition constraints the similar way).
        */
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
    private String buildQueryCls(int queryId) {
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
        // Create URL object. This operation may throw 'MalformedURLException'
        URL url = new URL(query);

        // Connect to the Ignite server, send query and get raw response
        BufferedReader reader = null;
        String responseRaw = null;
        try {
            StringBuilder sb = new StringBuilder();
            reader = new BufferedReader(new InputStreamReader(url.openStream()));
            String responseLine;
            while ((responseLine = reader.readLine()) != null) {
                sb.append(responseLine);
            }
            responseRaw = sb.toString();
            if (LOG.isDebugEnabled()) {
                LOG.debug("sendRestRequest(): URL: '" + query + "'; Result: '" + responseRaw + "'");
            }
        }
        catch (Exception e) {
            LOG.error("sendRestRequest(): Failed (connection failure). URL is '" + query + "'");
            throw e;
        }
        finally {
            if (reader != null) {
                reader.close();
            }
            // if 'reader' is null, an exception must have been thrown
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

    /**
     * Send an INSERT REST request to the Ignite server.
     *
     * Note that
     *
     * The {@link sendRestRequest()} is used to handle network operations, thus all its exceptions may be thrown. They are:
     * @throws ProtocolException if Ignite reports error in it's JSON response
     * @throws MalformedURLException if URL is malformed
     * @throws IOException in case of connection failure
     */
    private void sendInsertRestRequest(String query) throws ProtocolException, MalformedURLException, IOException {
        if (query == null) {
            LOG.error("sendInsertRestRequest(): Failed (malformed URL). URL is null");
            throw new MalformedURLException("sendInsertRestRequest(): query is null");
        }
        if (bufferWrite.isEmpty()) {
            return;
        }

        StringBuilder sb = new StringBuilder(query);
        String fieldDivisor = "";
        for (OneRow row : bufferWrite) {
            sb.append(fieldDivisor);
            fieldDivisor = ", ";
            sb.append((String)row.getData());
        }
        bufferWrite.clear();

        // Send REST request 'qryfldexe' to Ignite
        JsonElement response = sendRestRequest(buildQueryFldexe(sb.toString(), null));
        // Close the request immediately
        sendRestRequest(buildQueryCls(response.getAsJsonObject().get("queryId").getAsInt()));
    }
}
