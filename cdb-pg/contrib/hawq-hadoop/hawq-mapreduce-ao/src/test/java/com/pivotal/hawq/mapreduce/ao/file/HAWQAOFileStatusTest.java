package com.pivotal.hawq.mapreduce.ao.file;

import static org.junit.Assert.*;

import org.junit.Test;

import com.pivotal.hawq.mapreduce.HAWQException;

public class HAWQAOFileStatusTest
{

	@Test
	public void testHAWQAOFileStatusStringLongBooleanStringInt()
	{
		String pathStr = "/gpsql/gpseg0/16385/17673/17709.1";
		long fileLength = 979982384;
		boolean checksum = false;
		String compressType = "none";
		int blockSize = 32768;
		HAWQAOFileStatus aofilestatus = new HAWQAOFileStatus(pathStr,
				fileLength, checksum, compressType, blockSize);
		assertFalse(aofilestatus.getChecksum());
		assertEquals(aofilestatus.getBlockSize(), blockSize);
		assertEquals(aofilestatus.getCompressType(), compressType);
		assertEquals(aofilestatus.getFileLength(), fileLength);
		assertEquals(aofilestatus.getPathStr(), pathStr);
		assertEquals(aofilestatus.toString(), pathStr + "|" + fileLength + "|"
				+ checksum + "|" + compressType + "|" + blockSize);
	}

	@Test
	public void testHAWQAOFileStatusWithCorrectString()
	{
		String value = "/gpsql/gpseg0/16385/17673/17670.1|1243243|true|zlib|10000";
		HAWQException exception = null;
		HAWQAOFileStatus aofilestatus = null;
		try
		{
			aofilestatus = new HAWQAOFileStatus(value);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNull(exception);
		assertTrue(aofilestatus.getChecksum());
		assertEquals(aofilestatus.getBlockSize(), 10000);
		assertEquals(aofilestatus.getCompressType(), "zlib");
		assertEquals(aofilestatus.getFileLength(), 1243243);
		assertEquals(aofilestatus.getPathStr(),
				"/gpsql/gpseg0/16385/17673/17670.1");
		assertEquals(aofilestatus.toString(), value);
	}

	@Test
	public void testHAWQAOFileStatusWithWrongString1()
	{
		String value = "/gpsql/gpseg0/16385/17673/17670.1|1243243|true|zlib10000";
		HAWQException exception = null;
		try
		{
			new HAWQAOFileStatus(value);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

	@Test
	public void testHAWQAOFileStatusWithWrongString2()
	{
		String value = "/gpsql/gpseg0/16385/17673/17670.1|12432a3|true|zlib!10000";
		HAWQException exception = null;
		try
		{
			new HAWQAOFileStatus(value);
		}
		catch (HAWQException e)
		{
			exception = e;
		}
		assertNotNull(exception);
	}

}
