package org.apache.hawq.ranger.integration.service.tests;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Map;

public class RPSResponse {

    @JsonProperty
    public int requestId;

    @JsonProperty
    public List<Map<String, Object>> access;

    public List<Map<String, Object>> getAccess() {
        return access;
    }

    public boolean hasAccess() {
        return (boolean) access.get(0).get("allowed");
    }
}
