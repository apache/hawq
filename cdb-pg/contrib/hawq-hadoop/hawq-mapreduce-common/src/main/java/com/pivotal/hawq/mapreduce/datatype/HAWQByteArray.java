package com.pivotal.hawq.mapreduce.datatype;

import java.util.Arrays;

/*
 * GPSQL-938
 * 
 * Add a class to handle bytea type in database
 */
public class HAWQByteArray
{
	private byte[] bytes = null;

	/**
	 * Initialize a bytea from byte array
	 * 
	 * @param bytes
	 *            byte array that bytea init from
	 */
	public HAWQByteArray(byte[] bytes)
	{
		this.bytes = bytes;
	}

	/**
	 * Initialize a bytea from byte array
	 * 
	 * @param bytes
	 *            byte array that bytea init from
	 * @param offset
	 *            offset in byte array
	 * @param length
	 *            length in byte array
	 */
	public HAWQByteArray(byte[] bytes, int offset, int length)
	{
		this.bytes = new byte[length];
		System.arraycopy(bytes, offset, this.bytes, 0, length);
	}

	/**
	 * Get byte[]
	 * 
	 * @return byte array store in this HAWQByteArray
	 */
	public byte[] getBytes()
	{
		return bytes;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQByteArray)
		{
			HAWQByteArray other = (HAWQByteArray) obj;
			return Arrays.equals(bytes, other.getBytes());
		}
		return false;
	}

	@Override
	public String toString()
	{
		return new String(bytes);
	}
}
