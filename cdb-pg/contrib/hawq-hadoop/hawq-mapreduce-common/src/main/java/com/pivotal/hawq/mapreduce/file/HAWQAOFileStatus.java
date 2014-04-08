package com.pivotal.hawq.mapreduce.file;

/**
 * Extends {@link HAWQFileStatus} and add particular attribute of file for
 * append only files.
 */
public final class HAWQAOFileStatus extends HAWQFileStatus
{
	private boolean checksum;
	private String compressType = null;
	private int blockSize;

	/**
	 * Constructor
	 * 
	 * @param filePath path
	 * @param fileLength file length
	 * @param checksum checksum
	 * @param compressType compress type, acceptable values are none | zlib | quicklz
	 * @param blockSize block size
	 */
	public HAWQAOFileStatus(String filePath, long fileLength, boolean checksum,
			String compressType, int blockSize)
	{
		super(filePath, fileLength);
		this.checksum = checksum;
		this.compressType = compressType;
		this.blockSize = blockSize;

		if (!("none".equals(compressType) || "zlib".equals(compressType) || "quicklz".equals(compressType)))
			throw new UnsupportedOperationException("CompessType " + compressType + " is not supported");
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
		return blockSize;
	}

	@Override
	public String toString()
	{
		return filePath + "|" + fileLength + "|" + checksum + "|" + compressType + "|" + blockSize;
	}
}
