package com.pivotal.pxf.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ReflectionUtils.class})
public class UtilitiesTest {
	
	@Test
	public void getCodecClassNoName() {
		
		Configuration conf = new Configuration();
		String name = "some.bad.codec";
		
		try {
			Utilities.getCodec(conf, name);
			fail("function should fail with bad codec name " + name);
		}
		catch (IllegalArgumentException e) {
			assertEquals(e.getMessage(), "Compression codec " + name + " was not found.");
		}
	}
	
	@Test
	public void getCodecClassNoConf() {
		
		Configuration conf = null;
		String name = "org.apache.hadoop.io.compress.GzipCodec";
		
		try {
			Utilities.getCodec(conf, name);
			fail("function should fail with when conf is null");
		} catch (NullPointerException e) {
			assertTrue(true);
		}
	}
	
	@Test
	public void getCodecClassGzip() {
		
		Configuration conf = new Configuration();
		String name = "org.apache.hadoop.io.compress.GzipCodec";
		
		PowerMockito.mockStatic(ReflectionUtils.class);
		GzipCodec gzipCodec = mock(GzipCodec.class);
		
		when(ReflectionUtils.newInstance(GzipCodec.class, conf)).thenReturn(gzipCodec);
		
		CompressionCodec codec = Utilities.getCodec(conf, name);
		assertNotNull(codec);
		assertEquals(codec, gzipCodec);
	}
}
