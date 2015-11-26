package com.pivotal.hawq.mapreduce.ao.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public final class CompressZlib
{

	/**
	 * Compress
	 * 
	 * @param data
	 *            data to compress
	 * @param level
	 *            compress level
	 * @return byte[] data after compress
	 */
	public static byte[] compress(byte[] data, int level)
	{
		byte[] output = new byte[0];

		Deflater compresser = new Deflater(level);

		compresser.reset();
		compresser.setInput(data);
		compresser.finish();
		ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
		try
		{
			byte[] buf = new byte[1024];
			while (!compresser.finished())
			{
				int i = compresser.deflate(buf);
				bos.write(buf, 0, i);
			}
			output = bos.toByteArray();
		}
		catch (Exception e)
		{
			output = data;
			e.printStackTrace();
		}
		finally
		{
			try
			{
				bos.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		compresser.end();
		return output;
	}

	/**
	 * Compress
	 * 
	 * @param data
	 *            data to compress
	 * @param os
	 *            output stream
	 */
	public static void compress(byte[] data, OutputStream os)
	{
		DeflaterOutputStream dos = new DeflaterOutputStream(os);

		try
		{
			dos.write(data, 0, data.length);

			dos.finish();

			dos.flush();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * uncompress
	 * 
	 * @param data
	 *            data to uncompress
	 * @return byte[] data after uncompress
	 */
	public static byte[] decompress(byte[] data)
	{
		byte[] output = new byte[0];

		Inflater decompresser = new Inflater();
		decompresser.reset();
		decompresser.setInput(data);

		ByteArrayOutputStream o = new ByteArrayOutputStream(data.length);
		try
		{
			byte[] buf = new byte[1024];
			while (!decompresser.finished())
			{
				int i = decompresser.inflate(buf);
				o.write(buf, 0, i);
			}
			output = o.toByteArray();
		}
		catch (Exception e)
		{
			output = data;
			e.printStackTrace();
		}
		finally
		{
			try
			{
				o.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}

		decompresser.end();
		return output;
	}

	/**
	 * uncompress
	 * 
	 * @param is
	 *            input stream
	 * @return byte array output stream
	 */
	public static ByteArrayOutputStream decompress(InputStream is)
	{
		InflaterInputStream iis = new InflaterInputStream(is);
		ByteArrayOutputStream o = new ByteArrayOutputStream(1024);
		try
		{
			int i = 1024;
			byte[] buf = new byte[i];

			while ((i = iis.read(buf, 0, i)) > 0)
			{
				o.write(buf, 0, i);
			}

		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return o;
	}

	/**
	 * uncompress
	 * 
	 * @param is
	 *            input stream
	 * @param length
	 * @return byte array output stream
	 */
	public static ByteArrayOutputStream decompress(InputStream is, int length)
	{
		InflaterInputStream iis = new InflaterInputStream(is);
		ByteArrayOutputStream o = new ByteArrayOutputStream(1024);
		try
		{
			int i = 1024;
			byte[] buf = new byte[i];

			while ((i = iis.read(buf, 0, length)) > 0)
			{
				o.write(buf, 0, i);
				length -= i;
			}

		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return o;
	}
}
