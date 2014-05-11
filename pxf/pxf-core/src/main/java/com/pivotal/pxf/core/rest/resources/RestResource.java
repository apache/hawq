package com.pivotal.pxf.core.rest.resources;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Super of all PXF REST classes
 */
public abstract class RestResource {

    /**
     * Converts the request headers multivalued map to a case-insensitive regular map
     * by taking only first values and storing them in a CASE_INSENSITIVE_ORDER TreeMap.
     *
     * @param requestHeaders request headers multi map.
     * @return a regular case-insensitive map.
     */
    public Map<String, String> convertToCaseInsensitiveMap(MultivaluedMap<String, String> requestHeaders) {
        Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            if (values != null) {
                String value = values.get(0);
                if (value != null) {
                    result.put(key, value.replace("\\\"", "\""));
                }
            }
        }
        return result;
    }
}
