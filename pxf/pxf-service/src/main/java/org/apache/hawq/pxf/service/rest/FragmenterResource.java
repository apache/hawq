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

import org.apache.hawq.pxf.api.Fragment;
import org.apache.hawq.pxf.api.Fragmenter;
import org.apache.hawq.pxf.api.FragmentsStats;
import org.apache.hawq.pxf.service.FragmenterFactory;
import org.apache.hawq.pxf.service.FragmentsResponse;
import org.apache.hawq.pxf.service.FragmentsResponseFormatter;
import org.apache.hawq.pxf.service.utilities.AnalyzeUtils;
import org.apache.hawq.pxf.service.utilities.ProtocolData;
import org.apache.hawq.pxf.service.utilities.SecuredHDFS;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

/**
 * Class enhances the API of the WEBHDFS REST server. Returns the data fragments
 * that a data resource is made of, enabling parallel processing of the data
 * resource. Example for querying API FRAGMENTER from a web client
 * {@code curl -i "http://localhost:50070/pxf/{version}/Fragmenter/getFragments?path=/dir1/dir2/*txt"}
 * <code>/pxf/</code> is made part of the path when there is a webapp by that
 * name in tomcat.
 */
@Path("/" + Version.PXF_PROTOCOL_VERSION + "/Fragmenter/")
public class FragmenterResource extends RestResource {
    private static final Log LOG = LogFactory.getLog(FragmenterResource.class);

    /**
     * The function is called when
     * {@code http://nn:port/pxf/{version}/Fragmenter/getFragments?path=...} is used.
     *
     * @param servletContext Servlet context contains attributes required by
     *            SecuredHDFS
     * @param headers Holds HTTP headers from request
     * @param path Holds URI path option used in this request
     * @return response object with JSON serialized fragments metadata
     * @throws Exception if getting fragments info failed
     */
    @GET
    @Path("getFragments")
    @Produces("application/json")
    public Response getFragments(@Context final ServletContext servletContext,
                                 @Context final HttpHeaders headers,
                                 @QueryParam("path") final String path)
            throws Exception {

        ProtocolData protData = getProtocolData(servletContext, headers, path);

        /* Create a fragmenter instance with API level parameters */
        final Fragmenter fragmenter = FragmenterFactory.create(protData);

        List<Fragment> fragments = fragmenter.getFragments();

        fragments = AnalyzeUtils.getSampleFragments(fragments, protData);

        FragmentsResponse fragmentsResponse = FragmentsResponseFormatter.formatResponse(
                fragments, path);

        return Response.ok(fragmentsResponse, MediaType.APPLICATION_JSON_TYPE).build();
    }

    /**
     * The function is called when
     * {@code http://nn:port/pxf/{version}/Fragmenter/getFragmentsStats?path=...} is
     * used.
     *
     * @param servletContext Servlet context contains attributes required by
     *            SecuredHDFS
     * @param headers Holds HTTP headers from request
     * @param path Holds URI path option used in this request
     * @return response object with JSON serialized fragments statistics
     * @throws Exception if getting fragments info failed
     */
    @GET
    @Path("getFragmentsStats")
    @Produces("application/json")
    public Response getFragmentsStats(@Context final ServletContext servletContext,
                                      @Context final HttpHeaders headers,
                                      @QueryParam("path") final String path)
            throws Exception {

        ProtocolData protData = getProtocolData(servletContext, headers, path);

        /* Create a fragmenter instance with API level parameters */
        final Fragmenter fragmenter = FragmenterFactory.create(protData);

        FragmentsStats fragmentsStats = fragmenter.getFragmentsStats();
        String response = FragmentsStats.dataToJSON(fragmentsStats);
        if (LOG.isDebugEnabled()) {
            LOG.debug(FragmentsStats.dataToString(fragmentsStats, path));
        }

        return Response.ok(response, MediaType.APPLICATION_JSON_TYPE).build();
    }

    private ProtocolData getProtocolData(final ServletContext servletContext,
                                         final HttpHeaders headers,
                                         final String path) throws Exception {

        if (LOG.isDebugEnabled()) {
            StringBuilder startMsg = new StringBuilder(
                    "FRAGMENTER started for path \"" + path + "\"");
            for (String header : headers.getRequestHeaders().keySet()) {
                startMsg.append(" Header: ").append(header).append(" Value: ").append(
                        headers.getRequestHeader(header));
            }
            LOG.debug(startMsg);
        }

        /* Convert headers into a case-insensitive regular map */
        Map<String, String> params = convertToCaseInsensitiveMap(headers.getRequestHeaders());

        /* Store protocol level properties and verify */
        ProtocolData protData = new ProtocolData(params);
        if (protData.getFragmenter() == null) {
            protData.protocolViolation("fragmenter");
        }
        SecuredHDFS.verifyToken(protData, servletContext);

        return protData;
    }
}
