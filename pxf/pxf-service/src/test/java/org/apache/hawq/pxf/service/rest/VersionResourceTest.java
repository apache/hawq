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


import static org.junit.Assert.assertEquals;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Test;

public class VersionResourceTest {

    @Test
    public void getProtocolVersion() throws Exception {

        VersionResource resource = new VersionResource();
        Response result = resource.getProtocolVersion();

        assertEquals(Response.Status.OK,
                Response.Status.fromStatusCode(result.getStatus()));
        assertEquals(
                "{ \"version\": \"" + Version.PXF_PROTOCOL_VERSION + "\"}",
                result.getEntity().toString());
        assertEquals(result.getMetadata().get(HttpHeaders.CONTENT_TYPE).get(0),
                MediaType.APPLICATION_JSON_TYPE);
    }
}
