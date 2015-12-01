package org.apache.hawq.pxf.service.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.hawq.pxf.service.WriteBridge;
import org.apache.hawq.pxf.service.utilities.ProtocolData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.sun.jersey.core.util.MultivaluedMapImpl;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ WritableResource.class })
public class WritableResourceTest {

    WritableResource writableResource;
    ServletContext servletContext;
    HttpHeaders headers;
    InputStream inputStream;
    MultivaluedMap<String, String> headersMap;
    Map<String, String> params;
    ProtocolData protData;
    WriteBridge bridge;

    @Test
    public void streamPathWithSpecialChars() throws Exception {
        // test path with special characters
        String path = "I'mso<bad>!";

        Response result = writableResource.stream(servletContext, headers,
                path, inputStream);

        assertEquals(Response.Status.OK,
                Response.Status.fromStatusCode(result.getStatus()));
        assertEquals("wrote 0 bulks to I.mso.bad..",
                result.getEntity().toString());
    }

    @Test
    public void streamPathWithRegularChars() throws Exception {
        // test path with regular characters
        String path = "whatCAN1tellYOU";

        Response result = writableResource.stream(servletContext, headers,
                path, inputStream);

        assertEquals(Response.Status.OK,
                Response.Status.fromStatusCode(result.getStatus()));
        assertEquals("wrote 0 bulks to " + path, result.getEntity().toString());
    }

    @Before
    public void before() throws Exception {
        writableResource = mock(WritableResource.class,
                Mockito.CALLS_REAL_METHODS);

        // mock input
        servletContext = mock(ServletContext.class);
        headers = mock(HttpHeaders.class);
        inputStream = mock(InputStream.class);
        // mock internal functions to do nothing
        headersMap = new MultivaluedMapImpl();
        when(headers.getRequestHeaders()).thenReturn(headersMap);
        params = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        when(writableResource.convertToCaseInsensitiveMap(headersMap)).thenReturn(
                params);
        protData = mock(ProtocolData.class);
        PowerMockito.whenNew(ProtocolData.class).withArguments(params).thenReturn(
                protData);
        bridge = mock(WriteBridge.class);
        PowerMockito.whenNew(WriteBridge.class).withArguments(protData).thenReturn(
                bridge);
        when(protData.isThreadSafe()).thenReturn(true);
        when(bridge.isThreadSafe()).thenReturn(true);
    }
}
