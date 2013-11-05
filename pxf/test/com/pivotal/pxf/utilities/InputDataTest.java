package com.pivotal.pxf.utilities;

import java.util.HashMap;
import java.util.Map;

import com.pivotal.pxf.exception.ProfileConfException;

import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.pivotal.pxf.format.OutputFormat;
import static com.pivotal.pxf.exception.ProfileConfException.NO_PROFILE_DEF;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class InputDataTest 
{
	Map<String, String> parameters;

    @Test
    public void inputDataCreated()
    {
		InputData input = new InputData(parameters);

		assertEquals(System.getProperty("greenplum.alignment"), "all");
		assertEquals(input.totalSegments(), 2);
		assertEquals(input.segmentId(), -44);
		assertEquals(input.outputFormat(), OutputFormat.FORMAT_TEXT);
		assertEquals(input.serverName(), "my://bags");
		assertEquals(input.serverPort(), -8020);
		assertFalse(input.hasFilter());
		assertNull(input.filterString());
		assertEquals(input.columns(), 0);
		assertEquals(input.getDataFragment(), -1);
		assertNull(input.getRecordkeyColumn());
		assertEquals(input.accessor(), "are");
		assertEquals(input.resolver(), "packed");
		assertNull(input.GetAvroFileSchema());
		assertEquals(input.tableName(), "i'm/ready/to/go");
		assertEquals(input.path(), "/i'm/ready/to/go");
		assertEquals(input.getUserProperty("i'm-standing-here"), "outside-your-door");
		assertEquals(input.getParametersMap(), parameters);
    }

	@Test
	public void inputDataCopied()
	{
		InputData input = new InputData(parameters);
		InputData copy = new InputData(input);
		assertEquals(copy.getParametersMap(), input.getParametersMap());
	}

    @Test
    public void profileWithDuplicateProperty()
    {
        parameters.put("X-GP-PROFILE", "HIVE");
        try
        {
            new InputData(parameters);
            fail("Duplicate property should throw ProfileConfException");
        }
        catch (IllegalArgumentException iae)
        {
            assertEquals("Profile 'HIVE' already defines: [ACCESSOR, RESOLVER]", iae.getMessage());
        }
    }

    @Test
    public void definedProfile()
    {
        parameters.put("X-GP-PROFILE", "HIVE");
        parameters.remove("X-GP-ACCESSOR");
        parameters.remove("X-GP-RESOLVER");
        InputData input = new InputData(parameters);
        assertEquals(input.getProperty("X-GP-FRAGMENTER"), "HiveDataFragmenter");
        assertEquals(input.accessor, "HiveAccessor");
        assertEquals(input.resolver, "HiveResolver");
    }

    @Test
    public void undefinedProfile()
    {
        parameters.put("X-GP-PROFILE", "THIS_PROFILE_NEVER_EXISTED!");
        try
        {
            new InputData(parameters);
            fail("Undefined profile should throw ProfileConfException");
        }
        catch (ProfileConfException pce)
        {
            assertEquals(pce.getMsgFormat(), NO_PROFILE_DEF);
        }
    }
    
    @Test
    public void compressCodec()
    {
        parameters.put("X-GP-COMPRESSION_CODEC", "So I asked, who is he? He goes by the name of Wayne Rooney");   
        InputData input = new InputData(parameters);
        assertEquals(input.compressCodec, "So I asked, who is he? He goes by the name of Wayne Rooney");
    }

    @Test
    public void compressCodecBZip2()
    {
        parameters.put("X-GP-COMPRESSION_CODEC", "org.apache.hadoop.io.compress.BZip2Codec");
        try
        {
            new InputData(parameters);
            fail("org.apache.hadoop.io.compress.BZip2Codec should throw IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), "BZip2 compression is not supported");
        }
    }
    
    @Test
    public void compressType()
    {
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
        try
        {
            new InputData(parameters);
            fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), "Illegal compression type 'Oy'");
        }   

        parameters.put("X-GP-COMPRESSION_TYPE", "none");   
        try
        {
        	new InputData(parameters);
        	fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
        	assertEquals(e.getMessage(), "Illegal compression type 'NONE'. " +
        			"For disabling compression remove COMPRESSION_CODEC parameter.");
        }   	
    }
    
    /*
     * setUp function called before each test
	 */
	@Before
	public void setUp()
	{
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
		parameters.put("X-GP-I'M-STANDING-HERE", "outside-your-door");
	}
}
