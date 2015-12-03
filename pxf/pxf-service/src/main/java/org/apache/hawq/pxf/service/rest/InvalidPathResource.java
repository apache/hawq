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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.pxf.service.utilities.Utilities;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

/**
 * Class for catching paths that are not defined by other resources.
 * NOTE: This resource must be accessible without any security checks
 * as it is used to verify proper load of the PXF webapp.
 *
 * For each path, the version is compared to the current version PXF_VERSION.
 * The expected format of a path is "{@code http://<host>:<port>/pxf/<version>/<rest of path>}"
 *
 * The returned value is always a Server Error code (500).
 * If the version is different than the current version, an appropriate error is returned with version details.
 * Otherwise, an error about unknown path is returned.
 */
@Path("/")
public class InvalidPathResource {
    @Context
    UriInfo rootUri;

    private static final Log Log = LogFactory.getLog(InvalidPathResource.class);

    public InvalidPathResource() {
    }

    /**
     * Catches path /pxf/
     *
     * @return error message response
     */
    @GET
    @Path("/")
    public Response noPathGet() {
        return noPath();
    }

    /**
     * Catches path /pxf/
     *
     * @return error message response
     */
    @POST
    @Path("/")
    public Response noPathPost() {
        return noPath();
    }

    private Response noPath() {
        return sendErrorMessage(getUnknownPathMsg());
    }

    /**
     * Catches paths of pattern /pxf/*
     *
     * @param path request path
     * @return error message response
     */
    @GET
    @Path("/{path:.*}")
    public Response wrongPathGet(@PathParam("path") String path) {
        return wrongPath(path);
    }

    /**
     * Catches paths of pattern /pxf/*
     *
     * @param path request path
     * @return error message response
     */
    @POST
    @Path("/{path:.*}")
    public Response wrongPathPost(@PathParam("path") String path) {
        return wrongPath(path);
    }

    private Response wrongPath(String path) {

        String errmsg;
        String version = parseVersion(path);

        Log.debug("REST request: " + rootUri.getAbsolutePath() + ". " +
                "Version " + version + ", supported version is " + Version.PXF_PROTOCOL_VERSION);

        // if version is not of the format "v<number>" then it's not a version but a wrong path
        if (version.equals(Version.PXF_PROTOCOL_VERSION)  || !(version.matches("v[0-9]+"))) {
            errmsg = getUnknownPathMsg();
        } else {
            errmsg = "Wrong version " + version + ", supported version is " + Version.PXF_PROTOCOL_VERSION;
        }

        return sendErrorMessage(errmsg);
    }

    /**
     * Returns error message
     */
    private Response sendErrorMessage(String message) {
        ResponseBuilder b = Response.serverError();
        b.entity(message);
        b.type(MediaType.TEXT_PLAIN_TYPE);
        return b.build();
    }

    /**
     * Parses the version part from the path.
     * The the absolute path is
     * http://<host>:<port>/pxf/<version>/<rest of path>
     *
     * path - the path part after /pxf/
     * returns the first element after /pxf/
     */
    private String parseVersion(String path) {

        int slash = path.indexOf('/');
        if (slash == -1) {
            return path;
        }

        return path.substring(0, slash);
    }

    /**
     * Returns unknown path message, with the path's special characters masked.
     */
    private String getUnknownPathMsg() {
        return "Unknown path \"" + Utilities.maskNonPrintables(rootUri.getAbsolutePath().toString()) + "\"";
    }
}
