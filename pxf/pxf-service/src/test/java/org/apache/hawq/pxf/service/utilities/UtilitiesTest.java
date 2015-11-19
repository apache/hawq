package org.apache.hawq.pxf.service.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.apache.hawq.pxf.api.utilities.InputData;
import org.junit.Test;

public class UtilitiesTest {
    @Test
    public void byteArrayToOctalStringNull() throws Exception {
        StringBuilder sb = null;
        byte[] bytes = "nofink".getBytes();

        Utilities.byteArrayToOctalString(bytes, sb);

        assertNull(sb);

        sb = new StringBuilder();
        bytes = null;

        Utilities.byteArrayToOctalString(bytes, sb);

        assertEquals(0, sb.length());
    }

    @Test
    public void byteArrayToOctalString() throws Exception {
        String orig = "Have Narisha";
        String octal = "Rash Rash Rash!";
        String expected = orig + "\\\\122\\\\141\\\\163\\\\150\\\\040"
                + "\\\\122\\\\141\\\\163\\\\150\\\\040"
                + "\\\\122\\\\141\\\\163\\\\150\\\\041";
        StringBuilder sb = new StringBuilder();
        sb.append(orig);

        Utilities.byteArrayToOctalString(octal.getBytes(), sb);

        assertEquals(orig.length() + (octal.length() * 5), sb.length());
        assertEquals(expected, sb.toString());
    }

    @Test
    public void createAnyInstanceOldPackageName() throws Exception {

        InputData metaData = mock(InputData.class);

        try {
            Utilities.createAnyInstance(InputData.class,
                    "com.pivotal.pxf.Lucy", metaData);
            fail("creating an instance should fail because the class doesn't exist in classpath");
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
            assertEquals(
                    e.getMessage(),
                    "Class com.pivotal.pxf.Lucy doesn't not appear in classpath. "
                    + "Try changing the plugin name to start with \"org.apache.hawq.pxf\"");
        }
    }
}
