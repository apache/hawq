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

package org.apache.hawq.ranger.integration.service.tests;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RPSRequest {

    String user;
    Map<String, String> resources;
    List<String> privileges;

    public RPSRequest(String user,
                      Map<String, String> resources,
                      List<String> privileges) {
        this.user = user;
        this.resources = resources;
        this.privileges = privileges;
    }

    public String getJsonString()
            throws IOException {

        Map<String, Object> request = new HashMap<>();
        request.put("requestId", 9);
        request.put("user", user);
        request.put("clientIp", "123.0.0.21");
        request.put("context", "CREATE DATABASE sirotan;");
        Map<String, Object> accessHash = new HashMap<>();
        accessHash.put("resource", resources);
        accessHash.put("privileges", privileges);
        request.put("access", Arrays.asList(accessHash));
        return new ObjectMapper().writeValueAsString(request);
    }

}
