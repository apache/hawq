package com.pivotal.pxf.core.utilities;

import com.pivotal.pxf.api.OutputFormat;
import com.pivotal.pxf.core.utilities.ProtocolData;
import com.pivotal.pxf.api.utilities.ProfileConfException;
import static com.pivotal.pxf.api.utilities.ProfileConfException.MessageFormat.NO_PROFILE_DEF;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.servlet.ServletContext;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.security.UserGroupInformation;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UserGroupInformation.class})
public class ProtocolDataTest {
    Map<String, String> parameters;
    ServletContext mockContext;

    @Test
    public void ProtocolDataCreated() {
        ProtocolData protocolData = new ProtocolData(parameters);

        assertEquals(System.getProperty("greenplum.alignment"), "all");
        assertEquals(protocolData.totalSegments(), 2);
        assertEquals(protocolData.segmentId(), -44);
        assertEquals(protocolData.outputFormat(), OutputFormat.TEXT);
        assertEquals(protocolData.serverName(), "my://bags");
        assertEquals(protocolData.serverPort(), -8020);
        assertFalse(protocolData.hasFilter());
        assertNull(protocolData.filterString());
        assertEquals(protocolData.columns(), 0);
        assertEquals(protocolData.getDataFragment(), -1);
        assertNull(protocolData.getRecordkeyColumn());
        assertEquals(protocolData.accessor(), "are");
        assertEquals(protocolData.resolver(), "packed");
        assertNull(protocolData.getSchema());
        assertEquals(protocolData.dataSource(), "i'm/ready/to/go");
        assertEquals(protocolData.getUserProperty("i'm-standing-here"), "outside-your-door");
        assertEquals(protocolData.getParametersMap(), parameters);
        assertNull(protocolData.getLogin());
        assertNull(protocolData.getSecret());
    }

    @Test
    public void ProtocolDataCopied() {
        ProtocolData protocolData = new ProtocolData(parameters);
        ProtocolData copy = new ProtocolData(protocolData);
        assertEquals(copy.getParametersMap(), protocolData.getParametersMap());
    }

    @Test
    public void profileWithDuplicateProperty() {
        parameters.put("X-GP-PROFILE", "HIVE");
        try {
            new ProtocolData(parameters);
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
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.fragmenter(), "com.pivotal.pxf.plugins.hive.HiveDataFragmenter");
        assertEquals(protocolData.accessor(), "com.pivotal.pxf.plugins.hive.HiveAccessor");
        assertEquals(protocolData.resolver(), "com.pivotal.pxf.plugins.hive.HiveResolver");
    }

    @Test
    public void undefinedProfile() {
        parameters.put("X-GP-PROFILE", "THIS_PROFILE_NEVER_EXISTED!");
        try {
            new ProtocolData(parameters);
            fail("Undefined profile should throw ProfileConfException");
        } catch (ProfileConfException pce) {
            assertEquals(pce.getMsgFormat(), NO_PROFILE_DEF);
        }
    }

    @Test
    public void compressCodec() {
        parameters.put("X-GP-COMPRESSION_CODEC",
                "So I asked, who is he? He goes by the name of Wayne Rooney");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.compressCodec(),
                "So I asked, who is he? He goes by the name of Wayne Rooney");
    }

    @Test
    public void compressCodecBZip2() {
        parameters.put("X-GP-COMPRESSION_CODEC",
                "org.apache.hadoop.io.compress.BZip2Codec");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.compressCodec(),
                "org.apache.hadoop.io.compress.BZip2Codec");
    }

    @Test
    public void compressType() {
        parameters.put("X-GP-COMPRESSION_TYPE", "BLOCK");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.compressType(), "BLOCK");

        parameters.put("X-GP-COMPRESSION_TYPE", "ReCoRd");
        protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.compressType(), "RECORD");

        parameters.remove("X-GP-COMPRESSION_TYPE");
        protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.compressType(), "RECORD");

        parameters.put("X-GP-COMPRESSION_TYPE", "Oy");
        try {
            new ProtocolData(parameters);
            fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Illegal compression type 'Oy'");
        }

        parameters.put("X-GP-COMPRESSION_TYPE", "none");
        try {
            new ProtocolData(parameters);
            fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Illegal compression type 'NONE'. " + "For disabling compression remove COMPRESSION_CODEC parameter.");
        }
    }

    @Test
    public void threadSafeTrue() {
        parameters.put("X-GP-THREAD-SAFE", "TRUE");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.threadSafe(), true);

        parameters.put("X-GP-THREAD-SAFE", "true");
        protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.threadSafe(), true);
    }

    @Test
    public void threadSafeFalse() {
        parameters.put("X-GP-THREAD-SAFE", "False");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.threadSafe(), false);

        parameters.put("X-GP-THREAD-SAFE", "falSE");
        protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.threadSafe(), false);
    }

    @Test
    public void threadSafeMaybe() {
        parameters.put("X-GP-THREAD-SAFE", "maybe");
        try {
            new ProtocolData(parameters);
            fail("illegal THREAD-SAFE value should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Illegal boolean value 'maybe'. Usage: [TRUE|FALSE]");
        }
    }

    @Test
    public void threadSafeDefault() {
        parameters.remove("X-GP-THREAD-SAFE");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.threadSafe(), true);
    }

    @Test
    public void getFragmentMetadata() {
        ProtocolData protocolData = new ProtocolData(parameters);
        byte[] location = protocolData.getFragmentMetadata();
        assertEquals(new String(location), "Something in the way");
    }

    @Test
    public void getFragmentMetadataNull() {
        parameters.remove("X-GP-FRAGMENT-METADATA");
        ProtocolData ProtocolData = new ProtocolData(parameters);
        assertNull(ProtocolData.getFragmentMetadata());
    }

    @Test
    public void getFragmentMetadataNotBase64() {
        String badValue = "so b@d";
        parameters.put("X-GP-FRAGMENT-METADATA", badValue);
        try {
            new ProtocolData(parameters);
            fail("should fail with bad fragment metadata");
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Fragment metadata information must be Base64 encoded." +
                    "(Bad value: " + badValue + ")");
        }
    }

    @Test
    public void nullIdentifierThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);

        try {
            ProtocolData protocolData = new ProtocolData(parameters);
            fail("null X-GP-TOKEN-IDNT should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Internal server error. Property \"TOKEN-IDNT\" has no value in current request");
        }
    }
    
    @Test
    public void nullPasswordThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        parameters.put("X-GP-TOKEN-IDNT", "DEAD");

        try {
            ProtocolData protocolData = new ProtocolData(parameters);
            fail("null X-GP-TOKEN-PASS should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Internal server error. Property \"TOKEN-PASS\" has no value in current request");
        }
    }

    @Test
    public void nullKindThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        parameters.put("X-GP-TOKEN-IDNT", "DEAD");
        parameters.put("X-GP-TOKEN-PASS", "DEAD");

        try {
            ProtocolData protocolData = new ProtocolData(parameters);
            fail("null X-GP-TOKEN-KIND should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Internal server error. Property \"TOKEN-KIND\" has no value in current request");
        }
    }

    @Test
    public void nullServiceThrows() {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
        parameters.put("X-GP-TOKEN-IDNT", "DEAD");
        parameters.put("X-GP-TOKEN-PASS", "DEAD");
        parameters.put("X-GP-TOKEN-KIND", "DEAD");

        try {
            ProtocolData protocolData = new ProtocolData(parameters);
            fail("null X-GP-TOKEN-SRVC should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Internal server error. Property \"TOKEN-SRVC\" has no value in current request");
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

        PowerMockito.mockStatic(UserGroupInformation.class);
    }
}
