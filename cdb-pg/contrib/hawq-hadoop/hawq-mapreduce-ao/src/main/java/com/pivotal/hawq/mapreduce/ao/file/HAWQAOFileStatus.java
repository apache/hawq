package com.pivotal.hawq.mapreduce.ao.file;

import com.pivotal.hawq.mapreduce.HAWQException;
import com.pivotal.hawq.mapreduce.file.HAWQFileStatus;

public final class HAWQAOFileStatus extends HAWQFileStatus
{
	private boolean checksum;
	private String compressType = null;
	private int blocksize;

	/**
	 * Constructor
	 * 
	 * @param pathStr
	 *            path
	 * @param fileLength
	 *            file length
	 * @param checksum
	 *            checksum
	 * @param compressType
	 *            compress type
	 * @param blocksize
	 *            block size
	 */
	public HAWQAOFileStatus(String pathStr, long fileLength, boolean checksum,
			String compressType, int blocksize)
	{
		this.pathStr = pathStr;
		this.fileLength = fileLength;
		this.checksum = checksum;
		this.compressType = compressType;
		this.blocksize = blocksize;
	}

	/**
	 * Initialize a ao file status from a string
	 * 
	 * @param value
	 *            should be filePath|fileLength|checksum|compressType|blocksize
	 *            e.g.
	 *            /gpsql/gpseg0/16385/17673/17709.1|979982384|false|none|32768
	 *            /gpsql/gpseg0/16385/17673/17670.1|1243243|true|zlib|10000
	 * @throws HAWQException
	 */
	public HAWQAOFileStatus(String value) throws HAWQException
	{
		int sepPos1 = value.indexOf("|");
		int sepPos2 = value.indexOf("|", sepPos1 + 1);
		int sepPos3 = value.indexOf("|", sepPos2 + 1);
		int sepPos4 = value.indexOf("|", sepPos3 + 1);
		if (sepPos1 == -1 || sepPos2 == -1 || sepPos3 == -1 || sepPos4 == -1)
			throw new HAWQException("Cannot convert " + value
					+ " to HAWQAOFileStatus");

		try
		{
			fileLength = Long.parseLong(value.substring(sepPos1 + 1, sepPos2));
			blocksize = Integer.parseInt(value.substring(sepPos4 + 1));
		}
		catch (NumberFormatException e)
		{
			throw new HAWQException("Cannot convert " + value
					+ " to HAWQAOFileStatus");
		}

		pathStr = value.substring(0, sepPos1);
		checksum = Boolean.parseBoolean(value.substring(sepPos2 + 1, sepPos3));
		compressType = value.substring(sepPos3 + 1, sepPos4);
	}

	/**
	 * Get checksum of this file
	 * 
	 * @return checksum of this file
	 */
	public boolean getChecksum()
	{
		return checksum;
	}

	/**
	 * Get compress type of this file
	 * 
	 * @return compress type of this file
	 */
	public String getCompressType()
	{
		return compressType;
	}

	/**
	 * Get ao block size of this file
	 * 
	 * @return block size of this file
	 */
	public int getBlockSize()
	{
		return blocksize;
	}

	@Override
	public String toString()
	{
		return pathStr + "|" + fileLength + "|" + checksum + "|" + compressType
				+ "|" + blocksize;
	}
}
