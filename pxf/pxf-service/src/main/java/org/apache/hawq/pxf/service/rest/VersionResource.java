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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * PXF protocol version. Any call to PXF resources should include the current
 * version e.g. {@code ...pxf/v14/Bridge}
 */
class Version {
    /**
     * Constant which holds current protocol version. Getting replaced with
     * actual value on build stage, using pxfProtocolVersion parameter from
     * gradle.properties
     */
    final static String PXF_PROTOCOL_VERSION = "@pxfProtocolVersion@";

    public Version() {
    }

    public String version;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}

/**
 * Class returning the protocol version used by PXF.
 *
 * The expected format of a path is "
 * {@code http://<host>:<port>/pxf/ProtocolVersion}" The expected response is "
 * {@code PXF protocol version <version>}"
 *
 */
@Path("/ProtocolVersion")
public class VersionResource {

    private static final Log LOG = LogFactory.getLog(VersionResource.class);

    public VersionResource() {
    }

    /**
     * Returns the PXF protocol version used currently.
     *
     * @return response with the PXF protocol version
     */
    @GET
    @Produces("application/json")
    public Response getProtocolVersion() {

        ResponseBuilder b = Response.ok();
        b.entity("{ \"version\": \"" + Version.PXF_PROTOCOL_VERSION + "\"}");
        b.type(MediaType.APPLICATION_JSON_TYPE);
        return b.build();
    }
}
