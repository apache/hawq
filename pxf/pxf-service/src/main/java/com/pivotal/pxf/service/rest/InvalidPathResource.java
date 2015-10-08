package com.pivotal.pxf.service.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;


class Version {
    final static String PXF_PROTOCOL_VERSION = "v13";
}

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

    private Log Log;

    public InvalidPathResource() throws IOException {
        super();
        Log = LogFactory.getLog(InvalidPathResource.class);
    }

    /*
     * Catch path /pxf/
     */
    @GET
    @Path("/")
    public Response noPathGet() throws Exception {
        return noPath();
    }

    @POST
    @Path("/")
    public Response noPathPost() throws Exception {
        return noPath();
    }

    private Response noPath() throws Exception {
        String errmsg = "Unknown path " + rootUri.getAbsolutePath();
        return sendErrorMessage(errmsg);
    }

    /*
     * Catch paths of pattern /pxf/*
     */
    @GET
    @Path("/{path:.*}")
    public Response wrongPathGet(@PathParam("path") String path) throws Exception {
        return wrongPath(path);
    }

    /*
     * Catch paths of pattern /pxf/*
     */
    @POST
    @Path("/{path:.*}")
    public Response wrongPathPost(@PathParam("path") String path) throws Exception {
        return wrongPath(path);
    }


    private Response wrongPath(String path) throws Exception {

        String errmsg;
        String version = parseVersion(path);

        Log.debug("REST request: " + rootUri.getAbsolutePath() + ". " +
                "Version " + version + ", supported version is " + Version.PXF_PROTOCOL_VERSION);

        if (version.equals(Version.PXF_PROTOCOL_VERSION)) {
            errmsg = "Unknown path " + rootUri.getAbsolutePath();
        } else {
            errmsg = "Wrong version " + version + ", supported version is " + Version.PXF_PROTOCOL_VERSION;
        }

        return sendErrorMessage(errmsg);
    }

    /*
     * Return error message
     */
    private Response sendErrorMessage(String message) {
        ResponseBuilder b = Response.serverError();
        b.entity(message);
        b.type(MediaType.TEXT_PLAIN_TYPE);
        return b.build();
    }

    /*
     * Parse the version part from the path.
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
}
