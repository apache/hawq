package org.apache.hawq.pxf.service.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hawq.pxf.api.utilities.InputData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Class.class})
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
        String className = "com.pivotal.pxf.Lucy";
        ClassNotFoundException exception = new ClassNotFoundException(className);
        PowerMockito.mockStatic(Class.class);
        when(Class.forName(className)).thenThrow(exception);

        try {
            Utilities.createAnyInstance(InputData.class,
                    className, metaData);
            fail("creating an instance should fail because the class doesn't exist in classpath");
        } catch (Exception e) {
            assertEquals(e.getClass(), Exception.class);
            assertEquals(
                    e.getMessage(),
                    "Class " + className + " doesn't not appear in classpath. "
                    + "Plugins provided by PXF must start with \"org.apache.hawq.pxf\"");
        }
    }
}
