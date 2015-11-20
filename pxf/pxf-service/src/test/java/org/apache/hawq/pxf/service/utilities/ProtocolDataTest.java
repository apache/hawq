package org.apache.hawq.pxf.service.utilities;

import org.apache.hawq.pxf.api.OutputFormat;
import org.apache.hawq.pxf.api.utilities.ProfileConfException;

import static org.apache.hawq.pxf.api.utilities.ProfileConfException.MessageFormat.NO_PROFILE_DEF;

import org.apache.hawq.pxf.api.utilities.ProfilesConf;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.security.UserGroupInformation;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ UserGroupInformation.class, ProfilesConf.class })
public class ProtocolDataTest {
    Map<String, String> parameters;

    @Test
    public void protocolDataCreated() throws Exception {
        ProtocolData protocolData = new ProtocolData(parameters);

        assertEquals(System.getProperty("greenplum.alignment"), "all");
        assertEquals(protocolData.getTotalSegments(), 2);
        assertEquals(protocolData.getSegmentId(), -44);
        assertEquals(protocolData.outputFormat(), OutputFormat.TEXT);
        assertEquals(protocolData.serverName(), "my://bags");
        assertEquals(protocolData.serverPort(), -8020);
        assertFalse(protocolData.hasFilter());
        assertNull(protocolData.getFilterString());
        assertEquals(protocolData.getColumns(), 0);
        assertEquals(protocolData.getDataFragment(), -1);
        assertNull(protocolData.getRecordkeyColumn());
        assertEquals(protocolData.getAccessor(), "are");
        assertEquals(protocolData.getResolver(), "packed");
        assertEquals(protocolData.getDataSource(), "i'm/ready/to/go");
        assertEquals(protocolData.getUserProperty("i'm-standing-here"),
                "outside-your-door");
        assertEquals(protocolData.getParametersMap(), parameters);
        assertNull(protocolData.getLogin());
        assertNull(protocolData.getSecret());
    }

    @Test
    public void ProtocolDataCopied() throws Exception {
        ProtocolData protocolData = new ProtocolData(parameters);
        ProtocolData copy = new ProtocolData(protocolData);
        assertEquals(copy.getParametersMap(), protocolData.getParametersMap());
    }

    @Test
    public void profileWithDuplicateProperty() throws Exception {
        PowerMockito.mockStatic(ProfilesConf.class);

        Map<String, String> mockedProfiles = new HashMap<>();
        mockedProfiles.put("wHEn you trY yOUR bESt", "but you dont succeed");
        mockedProfiles.put("when YOU get WHAT you WANT",
                "but not what you need");
        mockedProfiles.put("when you feel so tired", "but you cant sleep");

        when(ProfilesConf.getProfilePluginsMap("a profile")).thenReturn(
                mockedProfiles);

        parameters.put("x-gp-profile", "a profile");
        parameters.put("when you try your best", "and you do succeed");
        parameters.put("WHEN you GET what YOU want", "and what you need");

        try {
            new ProtocolData(parameters);
            fail("Duplicate property should throw IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            assertEquals(
                    "Profile 'a profile' already defines: [when YOU get WHAT you WANT, wHEn you trY yOUR bESt]",
                    iae.getMessage());
        }
    }

    @Test
    public void definedProfile() throws Exception {
        parameters.put("X-GP-PROFILE", "HIVE");
        parameters.remove("X-GP-ACCESSOR");
        parameters.remove("X-GP-RESOLVER");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.getFragmenter(), "org.apache.hawq.pxf.plugins.hive.HiveDataFragmenter");
        assertEquals(protocolData.getAccessor(), "org.apache.hawq.pxf.plugins.hive.HiveAccessor");
        assertEquals(protocolData.getResolver(), "org.apache.hawq.pxf.plugins.hive.HiveResolver");
    }

    @Test
    public void undefinedProfile() throws Exception {
        parameters.put("X-GP-PROFILE", "THIS_PROFILE_NEVER_EXISTED!");
        try {
            new ProtocolData(parameters);
            fail("Undefined profile should throw ProfileConfException");
        } catch (ProfileConfException pce) {
            assertEquals(pce.getMsgFormat(), NO_PROFILE_DEF);
        }
    }

    @Test
    public void threadSafeTrue() throws Exception {
        parameters.put("X-GP-THREAD-SAFE", "TRUE");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.isThreadSafe(), true);

        parameters.put("X-GP-THREAD-SAFE", "true");
        protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.isThreadSafe(), true);
    }

    @Test
    public void threadSafeFalse() throws Exception {
        parameters.put("X-GP-THREAD-SAFE", "False");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.isThreadSafe(), false);

        parameters.put("X-GP-THREAD-SAFE", "falSE");
        protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.isThreadSafe(), false);
    }

    @Test
    public void threadSafeMaybe() throws Exception {
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
    public void threadSafeDefault() throws Exception {
        parameters.remove("X-GP-THREAD-SAFE");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertEquals(protocolData.isThreadSafe(), true);
    }

    @Test
    public void getFragmentMetadata() throws Exception {
        ProtocolData protocolData = new ProtocolData(parameters);
        byte[] location = protocolData.getFragmentMetadata();
        assertEquals(new String(location), "Something in the way");
    }

    @Test
    public void getFragmentMetadataNull() throws Exception {
        parameters.remove("X-GP-FRAGMENT-METADATA");
        ProtocolData ProtocolData = new ProtocolData(parameters);
        assertNull(ProtocolData.getFragmentMetadata());
    }

    @Test
    public void getFragmentMetadataNotBase64() throws Exception {
        String badValue = "so b@d";
        parameters.put("X-GP-FRAGMENT-METADATA", badValue);
        try {
            new ProtocolData(parameters);
            fail("should fail with bad fragment metadata");
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Fragment metadata information must be Base64 encoded."
                            + "(Bad value: " + badValue + ")");
        }
    }

    @Test
    public void nullTokenThrows() throws Exception {
        when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);

        try {
            new ProtocolData(parameters);
            fail("null X-GP-TOKEN should throw");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(),
                    "Internal server error. Property \"TOKEN\" has no value in current request");
        }
    }

    @Test
    public void filterUtf8() throws Exception {
        parameters.remove("X-GP-HAS-FILTER");
        parameters.put("X-GP-HAS-FILTER", "1");
        parameters.put("X-GP-FILTER", "UTF8_計算機用語_00000000");
        ProtocolData protocolData = new ProtocolData(parameters);
        assertTrue(protocolData.hasFilter());
        assertEquals("UTF8_計算機用語_00000000", protocolData.getFilterString());
    }

    @Test
    public void noStatsParams() {
        ProtocolData protData = new ProtocolData(parameters);

        assertEquals(0, protData.getStatsMaxFragments());
        assertEquals(0, protData.getStatsSampleRatio(), 0.1);
    }

    @Test
    public void statsParams() {
        parameters.put("X-GP-STATS-MAX-FRAGMENTS", "10101");
        parameters.put("X-GP-STATS-SAMPLE-RATIO", "0.039");

        ProtocolData protData = new ProtocolData(parameters);

        assertEquals(10101, protData.getStatsMaxFragments());
        assertEquals(0.039, protData.getStatsSampleRatio(), 0.01);
    }

    @Test
    public void statsMissingParams() {
        parameters.put("X-GP-STATS-MAX-FRAGMENTS", "13");
        try {
            new ProtocolData(parameters);
            fail("missing X-GP-STATS-SAMPLE-RATIO parameter");
        } catch (IllegalArgumentException e) {
            assertEquals(
                    e.getMessage(),
                    "Missing parameter: STATS-SAMPLE-RATIO and STATS-MAX-FRAGMENTS must be set together");
        }

        parameters.remove("X-GP-STATS-MAX-FRAGMENTS");
        parameters.put("X-GP-STATS-SAMPLE-RATIO", "1");
        try {
            new ProtocolData(parameters);
            fail("missing X-GP-STATS-MAX-FRAGMENTS parameter");
        } catch (IllegalArgumentException e) {
            assertEquals(
                    e.getMessage(),
                    "Missing parameter: STATS-SAMPLE-RATIO and STATS-MAX-FRAGMENTS must be set together");
        }
    }

    @Test
    public void statsSampleRatioNegative() {
        parameters.put("X-GP-STATS-SAMPLE-RATIO", "101");

        try {
            new ProtocolData(parameters);
            fail("wrong X-GP-STATS-SAMPLE-RATIO value");
        } catch (IllegalArgumentException e) {
            assertEquals(
                    e.getMessage(),
                    "Wrong value '101.0'. "
                            + "STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0");
        }

        parameters.put("X-GP-STATS-SAMPLE-RATIO", "0");
        try {
            new ProtocolData(parameters);
            fail("wrong X-GP-STATS-SAMPLE-RATIO value");
        } catch (IllegalArgumentException e) {
            assertEquals(
                    e.getMessage(),
                    "Wrong value '0.0'. "
                            + "STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0");
        }

        parameters.put("X-GP-STATS-SAMPLE-RATIO", "0.00005");
        try {
            new ProtocolData(parameters);
            fail("wrong X-GP-STATS-SAMPLE-RATIO value");
        } catch (IllegalArgumentException e) {
            assertEquals(
                    e.getMessage(),
                    "Wrong value '5.0E-5'. "
                            + "STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0");
        }

        parameters.put("X-GP-STATS-SAMPLE-RATIO", "a");
        try {
            new ProtocolData(parameters);
            fail("wrong X-GP-STATS-SAMPLE-RATIO value");
        } catch (NumberFormatException e) {
            assertEquals(e.getMessage(), "For input string: \"a\"");
        }
    }

    @Test
    public void statsMaxFragmentsNegative() {
        parameters.put("X-GP-STATS-MAX-FRAGMENTS", "10.101");

        try {
            new ProtocolData(parameters);
            fail("wrong X-GP-STATS-MAX-FRAGMENTS value");
        } catch (NumberFormatException e) {
            assertEquals(e.getMessage(), "For input string: \"10.101\"");
        }

        parameters.put("X-GP-STATS-MAX-FRAGMENTS", "0");

        try {
            new ProtocolData(parameters);
            fail("wrong X-GP-STATS-MAX-FRAGMENTS value");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Wrong value '0'. "
                    + "STATS-MAX-FRAGMENTS must be a positive integer");
        }
    }

    /*
     * setUp function called before each test
     */
    @Before
    public void setUp() {
        parameters = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);

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

        PowerMockito.mockStatic(UserGroupInformation.class);
    }

    /*
     * tearDown function called after each test
     */
    @After
    public void tearDown() {
        // Cleanup the system property ProtocolData sets
        System.clearProperty("greenplum.alignment");
    }
}
