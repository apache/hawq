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


import com.sun.jersey.core.util.MultivaluedMapImpl;

import org.apache.commons.codec.CharEncoding;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import javax.ws.rs.core.MultivaluedMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@PrepareForTest({RestResource.class})
public class RestResourceTest {

    RestResource restResource;
    MultivaluedMap<String, String> multivaluedMap;

    @Test
    public void testConvertToCaseInsensitiveMap() throws Exception {
        List<String> multiCaseKeys = Arrays.asList("X-GP-SHLOMO", "x-gp-shlomo", "X-Gp-ShLoMo");
        String value = "\\\"The king";
        String replacedValue = "\"The king";

        for (String key : multiCaseKeys) {
            multivaluedMap.put(key, Collections.singletonList(value));
        }

        assertEquals("All keys should have existed", multivaluedMap.keySet().size(), multiCaseKeys.size());

        Map<String, String> caseInsensitiveMap = restResource.convertToCaseInsensitiveMap(multivaluedMap);

        assertEquals("Only one key should have exist", caseInsensitiveMap.keySet().size(), 1);

        for (String key : multiCaseKeys) {
            assertEquals("All keys should have returned the same value", caseInsensitiveMap.get(key), replacedValue);
        }
    }

    @Test
    public void testConvertToCaseInsensitiveMapUtf8() throws Exception {
        byte[] bytes = {
                (byte) 0x61, (byte) 0x32, (byte) 0x63, (byte) 0x5c, (byte) 0x22,
                (byte) 0x55, (byte) 0x54, (byte) 0x46, (byte) 0x38, (byte) 0x5f,
                (byte) 0xe8, (byte) 0xa8, (byte) 0x88, (byte) 0xe7, (byte) 0xae,
                (byte) 0x97, (byte) 0xe6, (byte) 0xa9, (byte) 0x9f, (byte) 0xe7,
                (byte) 0x94, (byte) 0xa8, (byte) 0xe8, (byte) 0xaa, (byte) 0x9e,
                (byte) 0x5f, (byte) 0x30, (byte) 0x30, (byte) 0x30, (byte) 0x30,
                (byte) 0x30, (byte) 0x30, (byte) 0x30, (byte) 0x30, (byte) 0x5c,
                (byte) 0x22, (byte) 0x6f, (byte) 0x35
        };
        String value = new String(bytes, CharEncoding.ISO_8859_1);

        multivaluedMap.put("one", Collections.singletonList(value));

        Map<String, String> caseInsensitiveMap = restResource.convertToCaseInsensitiveMap(multivaluedMap);

        assertEquals("Only one key should have exist", caseInsensitiveMap.keySet().size(), 1);

        assertEquals("Value should be converted to UTF-8",
                caseInsensitiveMap.get("one"), "a2c\"UTF8_計算機用語_00000000\"o5");
    }

    @Before
    public void before() throws Exception {
        restResource = mock(RestResource.class, Mockito.CALLS_REAL_METHODS);
        multivaluedMap = new MultivaluedMapImpl();
    }
}
