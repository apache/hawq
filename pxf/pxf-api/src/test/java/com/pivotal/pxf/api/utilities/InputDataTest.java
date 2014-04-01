package com.pivotal.pxf.api.utilities;

import com.pivotal.pxf.api.OutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.servlet.ServletContext;
import java.util.HashMap;
import java.util.Map;

import static com.pivotal.pxf.api.utilities.ProfileConfException.MessageFormat.NO_PROFILE_DEF;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
public class InputDataTest {
    Map<String, String> parameters;
    ServletContext mockContext;

    @Test
    public void inputDataCreated() {
        InputData input = new InputData(parameters);

        assertEquals(System.getProperty("greenplum.alignment"), "all");
        assertEquals(input.totalSegments(), 2);
        assertEquals(input.segmentId(), -44);
        assertEquals(input.outputFormat(), OutputFormat.TEXT);
        assertEquals(input.serverName(), "my://bags");
        assertEquals(input.serverPort(), -8020);
        assertFalse(input.hasFilter());
        assertNull(input.filterString());
        assertEquals(input.columns(), 0);
        assertEquals(input.getDataFragment(), -1);
        assertNull(input.getRecordkeyColumn());
        assertEquals(input.accessor(), "are");
        assertEquals(input.resolver(), "packed");
        assertNull(input.getSchema());
        assertEquals(input.dataSource(), "i'm/ready/to/go");
        assertEquals(input.getUserProperty("i'm-standing-here"), "outside-your-door");
        assertEquals(input.getParametersMap(), parameters);
        assertNull(input.getLogin());
        assertNull(input.getSecret());
    }

    @Test
    public void inputDataCopied() {
        InputData input = new InputData(parameters);
        InputData copy = new InputData(input);
        assertEquals(copy.getParametersMap(), input.getParametersMap());
    }

    @Test
    public void profileWithDuplicateProperty() {
        parameters.put("X-GP-PROFILE", "HIVE");
        try {
            new InputData(parameters);
            fail("Duplicate property should throw IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals("Profile 'HIVE' already defines: [ACCESSOR, RESOLVER]",
                    iae.getMessage());
        }
    }

    @Test
    public void definedProfile() {
        parameters.put("X-GP-PROFILE", "HIVE");
        parameters.remove("X-GP-ACCESSOR");
        parameters.remove("X-GP-RESOLVER");
        InputData input = new InputData(parameters);
        assertEquals(input.getProperty("X-GP-FRAGMENTER"), "com.pivotal.pxf.plugins.hive.HiveDataFragmenter");
        assertEquals(input.accessor, "com.pivotal.pxf.plugins.hive.HiveAccessor");
        assertEquals(input.resolver, "com.pivotal.pxf.plugins.hive.HiveResolver");
    }

    @Test
    public void undefinedProfile() {
        parameters.put("X-GP-PROFILE", "THIS_PROFILE_NEVER_EXISTED!");
        try {
            new InputData(parameters);
            fail("Undefined profile should throw ProfileConfException");
        } catch (ProfileConfException pce) {
            assertEquals(pce.getMsgFormat(), NO_PROFILE_DEF);
        }
    }

    @Test
    public void compressCodec() {
        parameters.put("X-GP-COMPRESSION_CODEC",
                "So I asked, who is he? He goes by the name of Wayne Rooney");
        InputData input = new InputData(parameters);
        assertEquals(input.compressCodec,
                "So I asked, who is he? He goes by the name of Wayne Rooney");
    }

    @Test
    public void compressCodecBZip2() {
        parameters.put("X-GP-COMPRESSION_CODEC",
                "org.apache.hadoop.io.compress.BZip2Codec");
        InputData input = new InputData(parameters);
        assertEquals(input.compressCodec,
                "org.apache.hadoop.io.compress.BZip2Codec");
    }

    @Test
    public void compressType() {
        parameters.put("X-GP-COMPRESSION_TYPE", "BLOCK");
        InputData input = new InputData(parameters);
        assertEquals(input.compressType, "BLOCK");

        parameters.put("X-GP-COMPRESSION_TYPE", "ReCoRd");
        input = new InputData(parameters);
        assertEquals(input.compressType, "RECORD");

        parameters.remove("X-GP-COMPRESSION_TYPE");
        input = new InputData(parameters);
        assertEquals(input.compressType, "RECORD");

        parameters.put("X-GP-COMPRESSION_TYPE", "Oy");
        try {
            new InputData(parameters);
            fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Illegal compression type 'Oy'");
        }

        parameters.put("X-GP-COMPRESSION_TYPE", "none");
        try {
            new InputData(parameters);
            fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Illegal compression type 'NONE'. " + "For disabling compression remove COMPRESSION_CODEC parameter.");
        }
    }

    @Test
    public void threadSafeTrue() {
        parameters.put("X-GP-THREAD-SAFE", "TRUE");
        InputData input = new InputData(parameters);
        assertEquals(input.threadSafe, true);

        parameters.put("X-GP-THREAD-SAFE", "true");
        input = new InputData(parameters);
        assertEquals(input.threadSafe, true);
    }

    @Test
    public void threadSafeFalse() {
        parameters.put("X-GP-THREAD-SAFE", "False");
        InputData input = new InputData(parameters);
        assertEquals(input.threadSafe, false);

        parameters.put("X-GP-THREAD-SAFE", "falSE");
        input = new InputData(parameters);
        assertEquals(input.threadSafe, false);
    }

    @Test
    public void threadSafeMaybe() {
        parameters.put("X-GP-THREAD-SAFE", "maybe");
        try {
            new InputData(parameters);
            fail("illegal THREAD-SAFE value should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Illegal boolean value 'maybe'. Usage: [TRUE|FALSE]");
        }
    }

    @Test
    public void threadSafeDefault() {
        parameters.remove("X-GP-THREAD-SAFE");
        InputData input = new InputData(parameters);
        assertEquals(input.threadSafe, true);
    }

    @Test
    public void getFragmentMetadata() {
        InputData input = new InputData(parameters);
        byte[] location = input.getFragmentMetadata();
        assertEquals(new String(location), "Something in the way");
    }

    @Test
    public void getFragmentMetadataNull() {
        parameters.remove("X-GP-FRAGMENT-METADATA");
        InputData inputData = new InputData(parameters);
        assertNull(inputData.getFragmentMetadata());
    }

    @Test
    public void getFragmentMetadataNotBase64() {
        String badValue = "so b@d";
        parameters.put("X-GP-FRAGMENT-METADATA", badValue);
        try {
            new InputData(parameters);
            fail("should fail with bad fragment metadata");
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Fragment metadata information must be Base64 encoded." +
                    "(Bad value: " + badValue + ")");
        }
    }

    /*
     * setUp function called before each test
	 */
    @Before
    public void setUp() {
        parameters = new HashMap<String, String>();

        parameters.put("X-GP-ALIGNMENT", "all");
        parameters.put("X-GP-SEGMENT-ID", "-44");
        parameters.put("X-GP-SEGMENT-COUNT", "2");
        parameters.put("X-GP-HAS-FILTER", "0");
        parameters.put("X-GP-FORMAT", "TEXT");
        parameters.put("X-GP-URL-HOST", "my://bags");
        parameters.put("X-GP-URL-PORT", "-8020");
        parameters.put("X-GP-ATTRS", "-1");
        parameters.put("X-GP-ACCESSOR", "are");
        parameters.put("X-GP-RESOLVER", "packed");
        parameters.put("X-GP-DATA-DIR", "i'm/ready/to/go");
        parameters.put("X-GP-FRAGMENT-METADATA", "U29tZXRoaW5nIGluIHRoZSB3YXk=");
        parameters.put("X-GP-I'M-STANDING-HERE", "outside-your-door");

        mockContext = mock(ServletContext.class);
    }
}
