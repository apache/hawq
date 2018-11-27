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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Maps service exceptions to HTTP response.
 */
@Provider
public class ServiceExceptionMapper implements ExceptionMapper<Throwable> {

    private static final Log LOG = LogFactory.getLog(ServiceExceptionMapper.class);

    @Override
    public Response toResponse(Throwable e) {

        LOG.error("Service threw an exception: ", e);

        // default to internal server error (HTTP 500)
        Response.Status status = Response.Status.INTERNAL_SERVER_ERROR;

        if (e instanceof IllegalArgumentException) {
            status = Response.Status.BAD_REQUEST;
        }

        ErrorPayload error = new ErrorPayload(status.getStatusCode(), e.getMessage());

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Returning error response: status=%s message=%s",
                                    error.getStatus(), error.getMessage()));
        }

        return Response.status(status)
                .entity(error)
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    /**
     * Represents payload to be serialized as JSON into response.
     */
    public static class ErrorPayload {
        private int status;
        private String message;

        /**
         * Constructor.
         * @param status HTTP error status
         * @param message error message
         */
        public ErrorPayload(int status, String message) {
            this.status = status;
            this.message = message;
        }

        /**
         * Returns status code
         * @return status code
         */
        public int getStatus() {
            return status;
        }

        /**
         * Returns error message
         * @return error message
         */
        public String getMessage() {
            return message;
        }

    }
}