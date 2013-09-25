package com.pivotal.hawq.mapreduce.file;

/**
 * This class describe file status of database. This is an abstract class and
 * only store base attribute (path and length) of file.
 */
public abstract class HAWQFileStatus
{
	protected String pathStr = null;
	protected long fileLength;

	/**
	 * Get path string of this file
	 * 
	 * @return path
	 */
	public String getPathStr()
	{
		return pathStr;
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
