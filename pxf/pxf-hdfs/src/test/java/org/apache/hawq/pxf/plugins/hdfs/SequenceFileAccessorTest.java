package org.apache.hawq.pxf.plugins.hdfs;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.junit.Assert.*;

import org.apache.hawq.pxf.api.utilities.InputData;
import org.apache.hawq.pxf.plugins.hdfs.utilities.HdfsUtilities;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SequenceFileAccessor.class, HdfsSplittableDataAccessor.class, HdfsUtilities.class})
@SuppressStaticInitializationFor({"org.apache.hadoop.mapred.JobConf","org.apache.hadoop.fs.FileContext"}) // Prevents static inits
public class SequenceFileAccessorTest {

    InputData inputData;
    SequenceFileAccessor accessor;
    Map<String, String> parameters;
    ServletContext mockContext;
    Configuration hdfsConfiguration;
    SequenceFileInputFormat<?,?> inFormat;
    JobConf jobConf = null;
    Path file;
    FileSystem fs;
    FileContext fc;
    Log mockLog;

    /*
     * setUp function called before each test.
     * 
     * As the focus of the test is compression codec and type behavior, and
     * since the compression methods are private to SequenceFileAccessor, we
     * test their invocation and results by calling the public openForWrite().
     * Since openForWrite does some filesystem operations on HDFS, those had
     * to be mocked (and provided good material for a new Kafka story).
	 */
    @Before
    public void setUp() throws Exception {
    	
        mockContext = mock(ServletContext.class);
        inFormat = mock(SequenceFileInputFormat.class);
        hdfsConfiguration = mock(Configuration.class);
        jobConf = mock(JobConf.class);
        file = mock(Path.class);
        fs = mock(FileSystem.class);
        fc = mock(FileContext.class);
        inputData = mock(InputData.class);

    	PowerMockito.mockStatic(FileContext.class);
    	PowerMockito.mockStatic(HdfsUtilities.class);
		PowerMockito.whenNew(Path.class).withArguments(Mockito.anyString()).thenReturn(file);

        when(file.getFileSystem(any(Configuration.class))).thenReturn(fs);
        when(inputData.getDataSource()).thenReturn("deep.throat");
        when(inputData.getSegmentId()).thenReturn(0);             	
        when(FileContext.getFileContext()).thenReturn(fc);
    }

    /*
	 * After each test is done, close the accessor if it was created
	 */
    @After
    public void tearDown() throws Exception {
    	
        if (accessor == null) {
            return;
        }

        accessor.closeForWrite();
        accessor = null;
    }

    private void constructAccessor() throws Exception {
            	
        accessor = new SequenceFileAccessor(inputData);
        accessor.openForWrite();
    }
    
    private void mockCompressionOptions(String codec, String type)
    {
        when(inputData.getUserProperty("COMPRESSION_CODEC")).thenReturn(codec);
        when(inputData.getUserProperty("COMPRESSION_TYPE")).thenReturn(type);
    }
    
    @Test
    public void compressionNotSpecified() throws Exception {

    	mockCompressionOptions(null, null);
        constructAccessor();
        assertEquals(SequenceFile.CompressionType.NONE, accessor.getCompressionType());
        assertEquals(null, accessor.getCodec());
    }

	@Test
	public void compressCodec() throws Exception {

		//using BZip2 as a valid example
        when(HdfsUtilities.getCodec((Configuration)Mockito.anyObject(), Mockito.anyString())).thenReturn(new BZip2Codec());
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", null);
        constructAccessor();				
		assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
	}

    @Test
    public void bogusCompressCodec() {

    	final String codecName = "So I asked, who is he? He goes by the name of Wayne Rooney";
        when(HdfsUtilities.getCodec((Configuration)Mockito.anyObject(), Mockito.anyString())).thenThrow(new IllegalArgumentException("Compression codec " + codecName + " was not found."));
        mockCompressionOptions(codecName, null);
        
        try {
        	constructAccessor();
            fail("should throw no codec found exception");
        } catch (Exception e) {
            assertEquals("Compression codec " + codecName + " was not found.", e.getMessage());
        }
    }

	@Test
	public void compressTypes() throws Exception {

        when(HdfsUtilities.getCodec((Configuration)Mockito.anyObject(), Mockito.anyString())).thenReturn(new BZip2Codec());
        
        //proper value
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", "BLOCK");
        constructAccessor();				
		assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
		assertEquals(org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK, accessor.getCompressionType());

		//case (non) sensitivity
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", "ReCoRd");
        constructAccessor();				
		assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
		assertEquals(org.apache.hadoop.io.SequenceFile.CompressionType.RECORD, accessor.getCompressionType());

		//default (RECORD)
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", null);
        constructAccessor();				
		assertEquals(".bz2", accessor.getCodec().getDefaultExtension());
		assertEquals(org.apache.hadoop.io.SequenceFile.CompressionType.RECORD, accessor.getCompressionType());
	}

    @Test
	public void illegalCompressTypes() throws Exception {

        when(HdfsUtilities.getCodec((Configuration)Mockito.anyObject(), Mockito.anyString())).thenReturn(new BZip2Codec());
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", "Oy");
        
		try {
	        constructAccessor();				
			fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Illegal compression type 'Oy'", e.getMessage());
		}
		
        mockCompressionOptions("org.apache.hadoop.io.compress.BZip2Codec", "NONE");
        
		try {
	        constructAccessor();				
			fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals("Illegal compression type 'NONE'. For disabling compression remove COMPRESSION_CODEC parameter.", e.getMessage());
		}
		
	}
}
