package com.pivotal.pxf.resolvers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataInputStream;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pivotal.pxf.format.OneRow;
import com.pivotal.pxf.format.SimpleText;
import com.pivotal.pxf.utilities.InputData;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StringPassResolver.class, LogFactory.class})
public class StringPassResolverTest
{
	InputData inputData;
	DataInputStream inputStream;
	OngoingStubbing<Integer> ongoing;
	Log Log;

	@Test
	/*
	 * Test construction of StringPassResolver.
	 * 
	 * StringPassResolver is created and then members init is verified
	 */
	public void construction() throws Exception
	{
		inputData = mock(InputData.class);
		OneRow oneRow = mock(OneRow.class);
		PowerMockito.whenNew(OneRow.class).withNoArguments().thenReturn(oneRow);
		SimpleText simpleText = mock(SimpleText.class);
		PowerMockito.whenNew(SimpleText.class).withNoArguments().thenReturn(simpleText);
		
		StringPassResolver resolver = new StringPassResolver(inputData);
		PowerMockito.verifyNew(OneRow.class).withNoArguments();
		PowerMockito.verifyNew(SimpleText.class).withNoArguments();
	}
	
	@Test
	/*
	 * Test the setFields method: small \n terminated input
	 */
	public void testSetFields() throws Exception
	{
		
		StringPassResolver resolver = buildResolver(false);

		byte[] data = new byte[] {(int)'a', (int)'b', (int)'c', (int)'d', (int)'\n', 
                                  (int)'n', (int)'o', (int)'\n'};
		buildStream(data);

		OneRow oneRow = resolver.setFields(inputStream);
		
		verifyOneRow(oneRow, Arrays.copyOfRange(data, 0, 5));

		oneRow = resolver.setFields(inputStream);

		verifyOneRow(oneRow, Arrays.copyOfRange(data, 5, 8));

	}
	
	@Test
	/*
	 * Test the setFields method: input > buffer size, \n terminated
	 */
	public void testSetFieldsBigArray() throws Exception
	{
		
		StringPassResolver resolver = buildResolver(false);		
		
		byte[] bigArray = new byte[2000];
		for (int i = 0; i < 1999; ++i)
		{
			bigArray[i] = (byte)(i%10 + 30);
		}
		bigArray[1999] = (byte)'\n';
		
		buildStream(bigArray);
		
		OneRow oneRow = resolver.setFields(inputStream);
		
		verifyOneRow(oneRow, bigArray);
	
	}
	
	@Test
	/*
	 * Test the setFields method: input > buffer size, no \n
	 */
	public void testSetFieldsBigArrayNoNewLine() throws Exception
	{

		StringPassResolver resolver = buildResolver(true);
	
		byte[] bigArray = new byte[2000];
		for (int i = 0; i < 2000; ++i)
		{
			bigArray[i] = (byte)(i%10 + 60);
		}
		
		buildStream(bigArray);
		
		OneRow oneRow = resolver.setFields(inputStream);
		
		verifyOneRow(oneRow, bigArray);

		verifyWarning();	
	}
	
	@Test
	/*
	 * Test the setFields method: empty stream (returns -1)
	 */
	public void testSetFieldsEmptyStream() throws Exception
	{

		StringPassResolver resolver = buildResolver(true);
	
		byte[] empty = new byte[0]; 
		
		buildStream(empty);
		
		OneRow oneRow = resolver.setFields(inputStream);
		
		assertNull(oneRow);
	}
	
	/*
	 * helpers functions
	 */
	
	private StringPassResolver buildResolver(boolean hasWarn)
		throws Exception
	{
		// prepare log for warning
		if (hasWarn)
		{
			PowerMockito.mockStatic(LogFactory.class);
			Log = mock(Log.class);
			when(LogFactory.getLog(StringPassResolver.class)).thenReturn(Log);
		}
		
		inputData = mock(InputData.class);
		StringPassResolver resolver = new StringPassResolver(inputData);
		
		return resolver;
		
	}
	
	// add data to stream, end with -1 (eof)
	private DataInputStream buildStream(byte[] data) throws Exception
	{
		inputStream = mock(DataInputStream.class);
		ongoing = when(inputStream.read());
		for (byte b: data)
		{
			ongoing = ongoing.thenReturn((int)b);
		}
		ongoing.thenReturn(-1);
		return inputStream;
	}
	
	private void verifyOneRow(OneRow oneRow, byte[] expected)
	{
		assertNull(oneRow.getKey());
		SimpleText text = (SimpleText)oneRow.getData();
		byte[] result = Arrays.copyOfRange(text.getBytes(), 0, text.getLength());
		assertEquals(text.getLength(), expected.length);
		assertTrue(Arrays.equals(result, expected));
	}
	
	private void verifyWarning()
	{
		Mockito.verify(Log).warn("Stream ended without line break");
	}
}
