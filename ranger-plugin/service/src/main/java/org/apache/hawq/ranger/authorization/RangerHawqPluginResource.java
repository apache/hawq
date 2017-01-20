/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hawq.ranger.authorization;

import com.sun.jersey.spi.resource.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hawq.ranger.authorization.model.AuthorizationRequest;
import org.apache.hawq.ranger.authorization.model.AuthorizationResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Date;

/**
 * JAX-RS resource for the authorization endpoint.
 */
@Path("/")
@Singleton
public class RangerHawqPluginResource {

    private static final Log LOG = LogFactory.getLog(RangerHawqPluginResource.class);

    private HawqAuthorizer authorizer;
    private String version;

    /**
     * Constructor. Creates a new instance of the resource that uses <code>RangerHawqAuthorizer</code>.
     */
    public RangerHawqPluginResource() {
        this.authorizer = RangerHawqAuthorizer.getInstance();
    }


    @Path("/version")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response version()
    {
        String output = "{\"version\":\"" + Utils.getVersion() + "\"}";
        return Response.status(200).entity(output).build();
    }

    /**
     * Authorizes a request to access protected resources with requested privileges.
     * @param request authorization request
     * @return authorization response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public AuthorizationResponse authorize(AuthorizationRequest request)
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received authorization request: " + request);
        }

        // exceptions are handled by ServiceExceptionMapper
        AuthorizationResponse response = authorizer.isAccessAllowed(request);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Returning authorization response: " + response);
        }
        return response;
    }
}


