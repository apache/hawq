package com.pivotal.hawq.mapreduce.file;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HAWQAOFileStatusTest {

	@Test
	public void testHAWQAOFileStatusStringLongBooleanStringInt() {
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
		assertEquals(aofilestatus.getFilePath(), pathStr);
		assertEquals(aofilestatus.toString(), pathStr + "|" + fileLength + "|"
				+ checksum + "|" + compressType + "|" + blockSize);
	}

}
