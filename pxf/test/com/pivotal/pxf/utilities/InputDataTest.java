package com.pivotal.pxf.utilities;

import static com.pivotal.pxf.exception.ProfileConfException.NO_PROFILE_DEF;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.mockito.PowerMockito;

import com.pivotal.pxf.exception.ProfileConfException;
import com.pivotal.pxf.format.OutputFormat;

import javax.servlet.ServletContext;


@RunWith(PowerMockRunner.class)
@PrepareForTest({SecuredHDFS.class})
public class InputDataTest 
{
	Map<String, String> parameters;
	ServletContext mockContext;

    @Test
    public void inputDataCreated()
    {
		InputData input = new InputData(parameters, mockContext);

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
		assertEquals(input.getUserProperty("i'm-standing-here"),
					 "outside-your-door");
		assertEquals(input.getParametersMap(), parameters);

		PowerMockito.verifyStatic(times(1));
		SecuredHDFS.isDisabled();
    }

	@Test
	public void inputDataCopied()
	{
		InputData input = new InputData(parameters, mockContext);
		InputData copy = new InputData(input);
		assertEquals(copy.getParametersMap(), input.getParametersMap());
	}

	@Test
	public void profileWithDuplicateProperty() {
		parameters.put("X-GP-PROFILE", "HIVE");
		try {
			new InputData(parameters, mockContext);
			fail("Duplicate property should throw ProfileConfException");
		} catch (IllegalArgumentException iae) {
			assertEquals("Profile 'HIVE' already defines: [ACCESSOR, RESOLVER]",
						 iae.getMessage());
		}
	}

	@Test
	public void nullIdentifierThrows()
	{
		when(SecuredHDFS.isDisabled()).thenReturn(false);
		try {
            new InputData(parameters, mockContext);
			fail("null X-GP-TOKEN-IDNT should throw");
		} catch (IllegalArgumentException e)
		{
			assertEquals(e.getMessage(), 
						 "Internal server error. Property \"X-GP-TOKEN-IDNT\" " +
						 "has no value in current request");
		}
	}

	@Test
	public void definedProfile() {
		parameters.put("X-GP-PROFILE", "HIVE");
		parameters.remove("X-GP-ACCESSOR");
		parameters.remove("X-GP-RESOLVER");
		InputData input = new InputData(parameters, mockContext);
		assertEquals(input.getProperty("X-GP-FRAGMENTER"), "HiveDataFragmenter");
		assertEquals(input.accessor, "HiveAccessor");
		assertEquals(input.resolver, "HiveResolver");
	}

	@Test
	public void undefinedProfile() {
		parameters.put("X-GP-PROFILE", "THIS_PROFILE_NEVER_EXISTED!");
		try {
			new InputData(parameters, mockContext);
			fail("Undefined profile should throw ProfileConfException");
		} catch (ProfileConfException pce) {
			assertEquals(pce.getMsgFormat(), NO_PROFILE_DEF);
		}
	}

	public void invalidIdentifierThrows()
	{
		when(SecuredHDFS.isDisabled()).thenReturn(false);
		parameters.put("X-GP-TOKEN-IDNT", "This is odd");

		try {
            new InputData(parameters, mockContext);
			fail("invalid X-GP-TOKEN-IDNT should throw");
		} catch (IllegalArgumentException e)
		{
			assertEquals(e.getMessage(), 
						 "Internal server error. String This is odd isn't " +
						 "a valid hex string");
		}
	}

	@Test
	public void compressCodec() {
		parameters.put("X-GP-COMPRESSION_CODEC",
					   "So I asked, who is he? He goes by the name of Wayne Rooney");
		InputData input = new InputData(parameters, mockContext);
		assertEquals(input.compressCodec,
					 "So I asked, who is he? He goes by the name of Wayne Rooney");
	}

	@Test
	public void compressCodecBZip2() {
		parameters.put("X-GP-COMPRESSION_CODEC",
					   "org.apache.hadoop.io.compress.BZip2Codec");
		InputData input = new InputData(parameters, mockContext);
		assertEquals(input.compressCodec,
					 "org.apache.hadoop.io.compress.BZip2Codec");
	}

	@Test
	public void compressType() {
		parameters.put("X-GP-COMPRESSION_TYPE", "BLOCK");
		InputData input = new InputData(parameters, mockContext);
		assertEquals(input.compressType, "BLOCK");

		parameters.put("X-GP-COMPRESSION_TYPE", "ReCoRd");
		input = new InputData(parameters, mockContext);
		assertEquals(input.compressType, "RECORD");

		parameters.remove("X-GP-COMPRESSION_TYPE");
		input = new InputData(parameters, mockContext);
		assertEquals(input.compressType, "RECORD");

		parameters.put("X-GP-COMPRESSION_TYPE", "Oy");
		try {
			new InputData(parameters, mockContext);
			fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals(e.getMessage(), "Illegal compression type 'Oy'");
		}

		parameters.put("X-GP-COMPRESSION_TYPE", "none");
		try {
			new InputData(parameters, mockContext);
			fail("illegal COMPRESSION_TYPE should throw IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals(e.getMessage(),
						 "Illegal compression type 'NONE'. " + "For disabling compression remove COMPRESSION_CODEC parameter.");
		}
	}

	@Test
	public void threadSafeTrue() {
		parameters.put("X-GP-THREAD-SAFE", "TRUE");
		InputData input = new InputData(parameters, mockContext);
		assertEquals(input.threadSafe, true);

		parameters.put("X-GP-THREAD-SAFE", "true");
		input = new InputData(parameters, mockContext);
		assertEquals(input.threadSafe, true);
	}

	@Test
	public void threadSafeFalse() {
		parameters.put("X-GP-THREAD-SAFE", "False");
		InputData input = new InputData(parameters, mockContext);
		assertEquals(input.threadSafe, false);

		parameters.put("X-GP-THREAD-SAFE", "falSE");
		input = new InputData(parameters, mockContext);
		assertEquals(input.threadSafe, false);
	}

	@Test
	public void threadSafeMaybe() {
		parameters.put("X-GP-THREAD-SAFE", "maybe");
		try {
			new InputData(parameters, mockContext);
			fail("illegal THREAD-SAFE value should throw IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals(e.getMessage(),
						 "Illegal boolean value 'maybe'. Usage: [TRUE|FALSE]");
		}
	}

	@Test
	public void threadSafeDefault() {
		parameters.remove("X-GP-THREAD-SAFE");
		InputData input = new InputData(parameters, mockContext);
		assertEquals(input.threadSafe, true);
	}

	public void nullPasswordThrows()
	{
		when(SecuredHDFS.isDisabled()).thenReturn(false);
		parameters.put("X-GP-TOKEN-IDNT", "DEAD");

		try {
            new InputData(parameters, mockContext);
			fail("null X-GP-TOKEN-PASS should throw");
		} catch (IllegalArgumentException e)
		{
			assertEquals(e.getMessage(), 
						 "Internal server error. Property \"X-GP-TOKEN-PASS\" " +
						 "has no value in current request");
		}
	}

	@Test
	public void invalidPasswordThrows()
	{
		when(SecuredHDFS.isDisabled()).thenReturn(false);
		parameters.put("X-GP-TOKEN-IDNT", "DEAD");
		parameters.put("X-GP-TOKEN-PASS", "This is odd");

		try {
            new InputData(parameters, mockContext);
			fail("invalid X-GP-TOKEN-PASS should throw");
		} catch (IllegalArgumentException e)
		{
			assertEquals(e.getMessage(), 
						 "Internal server error. String This is odd isn't " +
						 "a valid hex string");
		}
	}

	@Test
	public void nullKindThrows()
	{
		when(SecuredHDFS.isDisabled()).thenReturn(false);
		parameters.put("X-GP-TOKEN-IDNT", "DEAD");
		parameters.put("X-GP-TOKEN-PASS", "DEAD");

		try {
            new InputData(parameters, mockContext);
			fail("null X-GP-TOKEN-KIND should throw");
		} catch (IllegalArgumentException e)
		{
			assertEquals(e.getMessage(), 
						 "Internal server error. Property \"X-GP-TOKEN-KIND\" " +
						 "has no value in current request");
		}
	}

	public void invalidKindThrows()
	{
		when(SecuredHDFS.isDisabled()).thenReturn(false);
		parameters.put("X-GP-TOKEN-IDNT", "DEAD");
		parameters.put("X-GP-TOKEN-PASS", "DEAD");
		parameters.put("X-GP-TOKEN-KIND", "This is odd");

		try {
            new InputData(parameters, mockContext);
			fail("invalid X-GP-TOKEN-KIND should throw");
		} catch (IllegalArgumentException e)
		{
			assertEquals(e.getMessage(), 
						 "Internal server error. String This is odd isn't " +
						 "a valid hex string");
		}
	}

	@Test
	public void nullServiceThrows()
	{
		when(SecuredHDFS.isDisabled()).thenReturn(false);
		parameters.put("X-GP-TOKEN-IDNT", "DEAD");
		parameters.put("X-GP-TOKEN-PASS", "DEAD");
		parameters.put("X-GP-TOKEN-KIND", "DEAD");

		try {
            new InputData(parameters, mockContext);
			fail("null X-GP-TOKEN-SRVC should throw");
		} catch (IllegalArgumentException e)
		{
			assertEquals(e.getMessage(), 
						 "Internal server error. Property \"X-GP-TOKEN-SRVC\" " +
						 "has no value in current request");
		}
	}

	@Test
	public void invalidServiceThrows()
	{
		when(SecuredHDFS.isDisabled()).thenReturn(false);
		parameters.put("X-GP-TOKEN-IDNT", "DEAD");
		parameters.put("X-GP-TOKEN-PASS", "DEAD");
		parameters.put("X-GP-TOKEN-KIND", "DEAD");
		parameters.put("X-GP-TOKEN-SRVC", "This is odd");

		try {
            new InputData(parameters, mockContext);
			fail("invalid X-GP-TOKEN-SRVC should throw");
		} catch (IllegalArgumentException e)
		{
			assertEquals(e.getMessage(), 
						 "Internal server error. String This is odd isn't " +
						 "a valid hex string");
		}
	}

	@Test
	public void inputDataVerifiesToken()
	{
		when(SecuredHDFS.isDisabled()).thenReturn(false);
		parameters.put("X-GP-TOKEN-IDNT", "DEAD");
		parameters.put("X-GP-TOKEN-PASS", "BEEF");
		parameters.put("X-GP-TOKEN-KIND", "BAAD");
		parameters.put("X-GP-TOKEN-SRVC", "F00D");

		new InputData(parameters, mockContext);

		PowerMockito.verifyStatic(times(1));
		SecuredHDFS.isDisabled();
		
		PowerMockito.verifyStatic(times(1));
		SecuredHDFS.verifyToken(new byte[]{(byte)0xde, (byte)0xad},
								new byte[]{(byte)0xbe, (byte)0xef},
								new byte[]{(byte)0xba, (byte)0xad},
								new byte[]{(byte)0xf0, (byte)0x0d},
								mockContext);
	}

	@Test
	public void getFragmentMetadata() {
		InputData input = new InputData(parameters, mockContext);
		byte[] location = input.getFragmentMetadata();
		assertEquals(new String(location), "Something in the way");
	}
	
	@Test
	public void getFragmentMetadataNull() {
		parameters.remove("X-GP-FRAGMENT-METADATA");
		InputData inputData = new InputData(parameters, mockContext);
		assertNull(inputData.getFragmentMetadata());
	}
	
	@Test
	public void getFragmentMetadataNotBase64() {
		String badValue = "so b@d";
		parameters.put("X-GP-FRAGMENT-METADATA", badValue);
		try {
			InputData inputData = new InputData(parameters, mockContext);
			fail("should fail with bad fragment metadata");
		}
		catch (Exception e) {
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

		PowerMockito.mockStatic(SecuredHDFS.class);
		when(SecuredHDFS.isDisabled()).thenReturn(true);
	}
}
