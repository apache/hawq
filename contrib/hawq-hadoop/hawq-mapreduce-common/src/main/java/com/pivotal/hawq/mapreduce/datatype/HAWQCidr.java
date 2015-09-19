package com.pivotal.hawq.mapreduce.datatype;

import com.pivotal.hawq.mapreduce.HAWQException;

/**
 * Store value of cidr in database
 */
public class HAWQCidr extends HAWQInet
{
	/**
	 * Initialize an cidr from byte array
	 * 
	 * @param inetType
	 *            ipv4 or ipv6
	 * @param bytes
	 *            byte array that cidr init from
	 * @param mask
	 *            net mask
	 * @throws HAWQException
	 *             length of bytes is not enough to init cidr or inetType is
	 *             invalid
	 */
	public HAWQCidr(InetType inetType, byte[] bytes, short mask)
			throws HAWQException
	{
		this(inetType, bytes, 0, mask);
	}

	/**
	 * Initialize an cidr from byte array
	 * 
	 * @param inetType
	 *            ipv4 or ipv6
	 * @param bytes
	 *            byte array that cidr init from
	 * @param offset
	 *            offset in byte array
	 * @param mask
	 *            net mask
	 * @throws HAWQException
	 *             length of bytes is not enough to init cidr or inetType is
	 *             invalid
	 */
	public HAWQCidr(InetType inetType, byte[] bytes, int offset, short mask)
			throws HAWQException
	{
		super(inetType, bytes, offset, mask);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof HAWQCidr)
		{
			HAWQInet other = (HAWQInet) obj;
			return super.equals(other);
		}
		return false;
	}

	@Override
	public String toString()
	{
		String temp = super.toString();
		if (temp.indexOf('/') == -1)
		{
			if (getInetType() == InetType.IPV4)
				return temp + "/32";
			else
				return temp + "/128";
		}
		return temp;
	}
}
