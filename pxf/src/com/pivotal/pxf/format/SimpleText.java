package com.pivotal.pxf.format;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

/*
 * Text class needs to be specialized since in the implementation of write() it serializes the 
 * underlying string while putting the string length at the beginning of the byte array, 
 * and this cannot be handled by the GPDB backend. 
 * We need the Text class because we want the Bridge to return Writable objects but on the other
 * hand we have a problem with the write() implementation in Text, hence the specialization to 
 * SimpleText
 */
public class SimpleText extends Text
{
	/*
	 * C'tor
	 */
	public SimpleText(Text text)
	{
		super(text);
	}

	/*
	 * C'tor
	 */
	public SimpleText()
	{
		super();
	}

	/*
	 * Overriding Text.write(), which is the whole purpose of this class. (see class notes)
	 */
	public void write(DataOutput out) throws IOException
	{
		byte[] bytes = getBytes();
		out.write(bytes, 0, getLength());
	}
}