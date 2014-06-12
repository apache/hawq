package com.pivotal.pxf.service.rest;

import com.sun.jersey.core.util.MultivaluedMapImpl;
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
    @Test
    public void testConvertToCaseInsensitiveMap() throws Exception {
        RestResource restResource = mock(RestResource.class, Mockito.CALLS_REAL_METHODS);
        MultivaluedMap<String, String> multivaluedMap = new MultivaluedMapImpl();
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
}
