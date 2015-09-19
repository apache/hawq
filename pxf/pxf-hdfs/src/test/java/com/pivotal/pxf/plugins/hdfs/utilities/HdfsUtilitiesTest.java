package com.pivotal.pxf.plugins.hdfs.utilities;

import com.pivotal.pxf.api.OneField;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor("com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities")
@PrepareForTest({HdfsUtilities.class, ReflectionUtils.class})
public class HdfsUtilitiesTest {

    Configuration conf;
    CompressionCodecFactory factory;
    Log Log;

    @Before
    public void SetupCompressionFactory() {
        factory = mock(CompressionCodecFactory.class);
        Whitebox.setInternalState(HdfsUtilities.class, factory);
        Log = mock(Log.class);
        Whitebox.setInternalState(HdfsUtilities.class, Log);
    }

    @Test
    public void getCodecNoName() {

        Configuration conf = new Configuration();
        String name = "some.bad.codec";

        try {
            HdfsUtilities.getCodec(conf, name);
            fail("function should fail with bad codec name " + name);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Compression codec " + name + " was not found.");
        }
    }

    @Test
    public void getCodecNoConf() {

        Configuration conf = null;
        String name = "org.apache.hadoop.io.compress.GzipCodec";

        try {
            HdfsUtilities.getCodec(conf, name);
            fail("function should fail with when conf is null");
        } catch (NullPointerException e) {
            assertTrue(true);
        }
    }

    @Test
    public void getCodecGzip() {

        Configuration conf = new Configuration();
        String name = "org.apache.hadoop.io.compress.GzipCodec";

        PowerMockito.mockStatic(ReflectionUtils.class);
        GzipCodec gzipCodec = mock(GzipCodec.class);

        when(ReflectionUtils.newInstance(GzipCodec.class, conf)).thenReturn(gzipCodec);

        CompressionCodec codec = HdfsUtilities.getCodec(conf, name);
        assertNotNull(codec);
        assertEquals(codec, gzipCodec);
    }

    @Test
    public void isThreadSafe() {

        testIsThreadSafe(
                "readable compression, no compression - thread safe",
                "/some/path/without.compression",
                null, null,
                true);

        testIsThreadSafe(
                "readable compression, gzip compression - thread safe",
                "/some/compressed/path.gz",
                null, new GzipCodec(),
                true);

        testIsThreadSafe(
                "readable compression, bzip2 compression - not thread safe",
                "/some/path/with/bzip2.bz2",
                null, new BZip2Codec(),
                false);

        testIsThreadSafe(
                "writable compression, no compression codec - thread safe",
                "/some/path",
                null, null,
                true);

        testIsThreadSafe(
                "writable compression, some compression codec - thread safe",
                "/some/path",
                "I.am.a.nice.codec", new NotSoNiceCodec(),
                true);

        testIsThreadSafe(
                "writable compression, compression codec bzip2 - not thread safe",
                "/some/path",
                "org.apache.hadoop.io.compress.BZip2Codec", new BZip2Codec(),
                false);
    }

    private void testIsThreadSafe(String testDescription, String path, String codecStr, CompressionCodec codec, boolean expectedResult) {
        prepareDataForIsThreadSafe(path, codecStr, codec);

        boolean result = HdfsUtilities.isThreadSafe(path, codecStr);
        assertTrue(testDescription, result == expectedResult);
    }

    private void prepareDataForIsThreadSafe(String dataDir, String codecStr, CompressionCodec codec) {
        try {
            conf = PowerMockito.mock(Configuration.class);
            PowerMockito.whenNew(Configuration.class).withNoArguments().thenReturn(conf);
        } catch (Exception e) {
            fail("new Configuration mocking failed");
        }

        if (codecStr == null) {
            when(factory.getCodec(new Path(dataDir))).thenReturn(codec);
        } else {
            PowerMockito.stub(PowerMockito.method(HdfsUtilities.class, "getCodecClass")).toReturn(codec.getClass());
        }
    }

    @Test
    public void isSplittableCodec() {

        testIsSplittableCodec("no codec - splittable",
                "some/innocent.file", null, true);
        testIsSplittableCodec("gzip codec - not splittable",
                "/gzip.gz", new GzipCodec(), false);
        testIsSplittableCodec("default codec - not splittable",
                "/default.deflate", new DefaultCodec(), false);
        testIsSplittableCodec("bzip2 codec - splittable",
                "bzip2.bz2", new BZip2Codec(), true);
    }

    private void testIsSplittableCodec(String description,
                                       String pathName, CompressionCodec codec, boolean expected) {
        Path path = new Path(pathName);
        when(factory.getCodec(path)).thenReturn(codec);

        boolean result = HdfsUtilities.isSplittableCodec(path);
        assertEquals(description, result, expected);
    }

    @Test
    public void testToString() {
        List<OneField> oneFields = Arrays.asList(new OneField(1, "uno"), new OneField(2, "dos"), new OneField(3, "tres"));

        assertEquals("uno!dos!tres", HdfsUtilities.toString(oneFields, "!"));

        assertEquals("uno", HdfsUtilities.toString(Collections.singletonList(oneFields.get(0)), "!"));

        assertEquals("", HdfsUtilities.toString(Collections.<OneField>emptyList(), "!"));
    }
}
