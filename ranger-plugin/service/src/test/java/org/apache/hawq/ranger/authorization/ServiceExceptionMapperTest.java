/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hawq.ranger.authorization;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ServiceExceptionMapperTest {

    private ServiceExceptionMapper mapper;

    @Before
    public void setup() {
        mapper = new ServiceExceptionMapper();
    }

    @Test
    public void testIllegalArgumentException() {

        Response response = mapper.toResponse(new IllegalArgumentException("reason"));
        ServiceExceptionMapper.ErrorPayload entity = (ServiceExceptionMapper.ErrorPayload) response.getEntity();

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), entity.getStatus());
        assertEquals("reason", entity.getMessage());
    }

    @Test
    public void testOtherException() {

        Response response = mapper.toResponse(new Exception("reason"));
        ServiceExceptionMapper.ErrorPayload entity = (ServiceExceptionMapper.ErrorPayload) response.getEntity();

        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), entity.getStatus());
        assertEquals("reason", entity.getMessage());
    }

}
