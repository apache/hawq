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
