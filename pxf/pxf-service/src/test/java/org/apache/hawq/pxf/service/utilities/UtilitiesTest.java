package org.apache.hawq.pxf.service.utilities;

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
                    "Class " + className + " does not appear in classpath. "
                    + "Plugins provided by PXF must start with \"org.apache.hawq.pxf\"");
        }
    }

    @Test
    public void maskNonPrintable() throws Exception {
        String input = "";
        String result = Utilities.maskNonPrintables(input);
        assertEquals("", result);

        input = null;
        result = Utilities.maskNonPrintables(input);
        assertEquals(null, result);

        input = "Lucy in the sky";
        result = Utilities.maskNonPrintables(input);
        assertEquals("Lucy.in.the.sky", result);

        input = "with <$$$@#$!000diamonds!!?!$#&%/>";
        result = Utilities.maskNonPrintables(input);
        assertEquals("with.........000diamonds......../.", result);

        input = "http://www.beatles.com/info?query=whoisthebest";
        result = Utilities.maskNonPrintables(input);
        assertEquals("http://www.beatles.com/info.query.whoisthebest", result);
    }
}
