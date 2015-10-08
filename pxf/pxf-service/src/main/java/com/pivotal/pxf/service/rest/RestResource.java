package com.pivotal.pxf.service.rest;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.codec.CharEncoding;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Super of all PXF REST classes
 */
public abstract class RestResource {

    private static Log Log = LogFactory.getLog(RestResource.class);

    /**
     * Converts the request headers multivalued map to a case-insensitive regular map
     * by taking only first values and storing them in a CASE_INSENSITIVE_ORDER TreeMap.
     * All values are converted from ISO_8859_1 (ISO-LATIN-1) to UTF_8.
     *
     * @param requestHeaders request headers multi map.
     * @return a regular case-insensitive map.
     * @throws UnsupportedEncodingException if the named charsets ISO_8859_1 and UTF_8 are not supported
     */
    public Map<String, String> convertToCaseInsensitiveMap(MultivaluedMap<String, String> requestHeaders)
            throws UnsupportedEncodingException {
        Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            if (values != null) {
                String value = values.get(0);
                if (value != null) {
                    // converting to value UTF-8 encoding
                    value = new String(value.getBytes(CharEncoding.ISO_8859_1), CharEncoding.UTF_8);
                    Log.trace("key: " + key + ". value: " + value);
                    result.put(key, value.replace("\\\"", "\""));
                }
            }
        }
        return result;
    }
}
