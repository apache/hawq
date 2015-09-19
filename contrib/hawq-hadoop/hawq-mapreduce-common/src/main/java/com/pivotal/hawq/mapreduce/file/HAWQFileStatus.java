package com.pivotal.hawq.mapreduce.file;

/**
 * This class describe file status of database.
 */
public class HAWQFileStatus
{
	protected String filePath = null;
	protected long fileLength;

	public HAWQFileStatus(String filePath, long fileLength) {
		this.filePath = filePath;
		this.fileLength = fileLength;
	}

	/**
	 * Get path string of this file
	 * 
	 * @return path
	 */
	public String getFilePath()
	{
		return filePath;
	}

	/**
	 * Get file length of this file
	 * 
	 * @return file length
	 */
	public long getFileLength()
	{
		return fileLength;
	}

}
