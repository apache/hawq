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

    private static final Log LOG = LogFactory.getLog(RestResource.class);

    /**
     * Converts the request headers multivalued map to a case-insensitive
     * regular map by taking only first values and storing them in a
     * CASE_INSENSITIVE_ORDER TreeMap. All values are converted from ISO_8859_1
     * (ISO-LATIN-1) to UTF_8.
     *
     * @param requestHeaders request headers multi map.
     * @return a regular case-insensitive map.
     * @throws UnsupportedEncodingException if the named charsets ISO_8859_1 and
     *             UTF_8 are not supported
     */
    public Map<String, String> convertToCaseInsensitiveMap(MultivaluedMap<String, String> requestHeaders)
            throws UnsupportedEncodingException {
        Map<String, String> result = new TreeMap<>(
                String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            if (values != null) {
                String value = values.get(0);
                if (value != null) {
                    // converting to value UTF-8 encoding
                    value = new String(value.getBytes(CharEncoding.ISO_8859_1),
                            CharEncoding.UTF_8);
                    LOG.trace("key: " + key + ". value: " + value);
                    result.put(key, value.replace("\\\"", "\""));
                }
            }
        }
        return result;
    }
}
